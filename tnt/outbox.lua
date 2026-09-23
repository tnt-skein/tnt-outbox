--- Ящик исходящих: событие пишется в спейс одной транзакцией с данными,
--- а уходит отдельным вывозом (`docs/outbox.md`).
---
---     local outbox = require('tnt.outbox')
---
---     outbox.configure({
---         senders = {
---             ['order.paid'] = { send = broker.send, features = broker.features },
---         },
---     })
---     outbox.start()
---
---     box.atomic(function()
---         orders:insert({ 11, 'оплачен' })
---         outbox.write('order.paid', { order = 11 })
---     end)
---
--- **«Сохранили и отправили» без ящика теряет событие** при обрыве между
--- двумя действиями, и потеря эта тихая. Ящик кладёт событие строкой спейса
--- в той же транзакции, что и данные: откат уносит обоих, фиксация
--- оставляет обоих, а вывоз отправляет строку и удаляет её после
--- подтверждения отправителя.
---
--- **Ящик нужен для отправки наружу** — брокер, вебхук, очередь другого
--- кластера. Такой отправитель ходит по сети, и звать его в транзакции
--- нельзя: уступка порвала бы её, а сообщение ушло бы и при откате.
---
--- **Отправитель приходит аргументом**, а не из контейнера:
--- `configure({ senders = { <имя> = { send = …, features = … } } })`.
--- Отправитель — таблица с `send(name, body, opts)` и признаками: по ним
--- запись отказывает сразу, а не через час на вывозе.
---
--- **Отказа парой у записи нет**: отказ box — узел для чтения, уступка
--- в транзакции — это отказ транзакции вызывающего, и он приходит к нему
--- так же, как отказ его собственной вставки. Исключение — и ошибка
--- программиста: незнакомая настройка, тело не из простых данных, имя
--- без отправителя.
---
--- **Гарантия — хотя бы раз**: повтор приходит после обрыва между
--- отправкой и удалением строки и после смены ведущего. Повтор несёт тот же
--- опознаватель, и отсекает его получатель по `message.id`.
---
--- **Зарытое возвращает `kick`**: после починки отправителя строка
--- из `outbox_dead` уходит в хвост ящика с тем же опознавателем и теми же
--- настройками отправки. Без возврата зарытое копилось бы мусором,
--- а ручная вставка в спейс шла бы мимо отметки фиксации.

local clock = require('tnt.clock')
local common = require('tnt.message')
local loop = require('tnt.loop')
local must = require('tnt.must')

local commit = require('tnt.outbox.commit')
local message = require('tnt.outbox.message')
local shipper = require('tnt.outbox.shipper')
local space_of = require('tnt.outbox.space')

local log = require('tnt.log').new('tnt.outbox')

local Module = {}

--- Как часто идёт вывоз, секунды. Будильник вывоза — отметка фиксации,
--- и такт нужен ради строк, о которых отметки нет: лежавших до запуска
--- и вставленных мимо записи.
Module.DEFAULT_INTERVAL = 1

--- Сколько зарытых отдаёт `dead()` без аргумента: столько человек прочитает
--- за раз, а остальное возьмёт следующим вызовом с большим числом.
Module.DEFAULT_DEAD = 100

--- Сколько зарытых возвращает `kick()` без аргумента: одну. Возврат —
--- решение человека о строке, которую он разобрал, и без числа оно
--- не должно касаться остальных.
Module.DEFAULT_KICK = 1

--- Больше скольких зарытых `kick` за раз не возвращает.
---
--- Возврат — одна транзакция без уступки, и пока она идёт, узел не
--- отвечает никому. Замер на Tarantool 3.8: тысяча строк — 0,03 с, десять
--- тысяч — 0,17 с, пятьдесят — около секунды, а двести тысяч упираются
--- в срез файбера и откатываются целиком. Больше тысячи возвращают
--- несколькими вызовами.
Module.MAX_KICK = 1000

--- Возврат зарытых в чужой транзакции. Он стал бы её частью, и её откат
--- молча вернул бы строки в зарытые, хотя вызывающий уже получил число
--- возвращённых.
local FOREIGN =
    'возврат зарытых идёт своей транзакцией, и в чужой его не зовут'

--- Имя назначения: буква, дальше буквы, цифры, подчёркивание, точка
--- и дефис — `order.paid`, `billing.invoice-sent`.
local NAME = '^%a[%w_.-]*$'

--- Настройки записи — это настройки `send` назначенного отправителя,
--- и признаки у них его: настройка, которой он не умеет, — исключение
--- при записи, а не отказ вывоза через час.
local WRITING = { what = 'настройки записи', cannot = 'отправитель не умеет' }

--- Настройки ящика.
local SETTINGS = { senders = '?table', interval = '?number', batch = '?integer', backoff = '?table' }

--- Настройки отступа вывоза — те же, что у `tnt-retry`. Разброс здесь
--- только долей: стратегия `decorrelated` считает паузу от прошлой, а
--- у вывоза между тактами её никто не держит.
local BACKOFF = { base = '?number', factor = '?number', jitter = '?number', max = '?number' }

--- Потолок множителя отступа. Меньше единицы множитель сокращал бы паузу
--- с каждым отказом — это не отступ, а разгон; сотня и так уводит вторую
--- паузу за потолок.
local MAX_FACTOR = 100

---@class TntOutboxSender Кому вывоз отдаёт сообщение
---@field send fun(name: string, body: any, opts: table): string|nil, any Отправка: опознаватель либо `nil, err`
---@field features table<string, boolean>|nil Что он умеет: `false` — не умеет

---@class TntOutboxSettings
---@field senders table<string, TntOutboxSender>|nil Отправители по именам назначений
---@field interval number|nil Как часто идёт вывоз, секунды
---@field batch integer|nil Сколько строк вывозится за проход
---@field backoff table|nil Отступ после повторяемого отказа

--- Такт вывоза. Собирается ниже, рядом с тем, что его запускает.
---@type any
local ticker

--- Настройки ящика; вывоз держит ту же таблицу.
---@type any
local settings

--- Счётчики ящика с нуля: записи и возвраты считает фасад, остальное —
--- вывоз.
---
--- Одним местом, а не двумя: запуск и `reset` ставят одни и те же нули,
--- а два перечисления однажды разошлись бы.
---@return table<string, integer>
local function zeroed()
    return { written = 0, kicked = 0 }
end

--- Счётчики записи и возврата.
local counts = zeroed()

--- Умолчания настроек.
---@return table
local function defaults()
    return {
        senders = {},
        interval = Module.DEFAULT_INTERVAL,
        batch = shipper.DEFAULT_BATCH,
        backoff = shipper.DEFAULT_BACKOFF,
    }
end

--- Настраивает ящик: кому отправлять, как часто и какими кусками.
---
--- Отправители заменяются целиком, а не дополняются: иначе снятый
--- отправитель оставался бы на месте, и перечитывание конфигурации
--- не могло бы его убрать.
---@param opts TntOutboxSettings|nil
function Module.configure(opts)
    local caller = must.at(2)
    local given = caller.optional.options(opts, 'настройки ящика', SETTINGS) or {}
    local backoff = caller.optional.options(given.backoff, 'настройки ящика.backoff', BACKOFF)
    local tuned = backoff or {}

    caller.optional.positive(given.interval, 'настройки ящика.interval')
    caller.optional.positive(given.batch, 'настройки ящика.batch')
    caller.optional.non_negative(tuned.base, 'настройки ящика.backoff.base')
    caller.optional.between(tuned.factor, 'настройки ящика.backoff.factor', 1, MAX_FACTOR)
    caller.optional.between(tuned.jitter, 'настройки ящика.backoff.jitter', 0, 1)
    caller.optional.non_negative(tuned.max, 'настройки ящика.backoff.max')

    -- Отправитель — чужой объект, а не таблица настроек: у него бывают
    -- свои поля, и перечня ключей ему не ставится. Спрашивается с него
    -- только отправка и признаки.
    for name, sender in pairs(given.senders or {}) do
        caller.matches(name, 'имя назначения', NAME)
        caller.table(sender, ('отправитель %s'):format(name))
        caller.callable(sender.send, ('отправитель %s.send'):format(name))
        caller.optional.table(sender.features, ('отправитель %s.features'):format(name))
    end

    settings = {
        senders = given.senders or {},
        interval = given.interval or Module.DEFAULT_INTERVAL,
        batch = given.batch or shipper.DEFAULT_BATCH,
        backoff = backoff or shipper.DEFAULT_BACKOFF,
    }

    ticker:set_interval(settings.interval)
    shipper.configure(settings)
end

--- Ящик, готовый к записи.
---
--- Заведение — DDL, и оно уступает: в чужой транзакции уступка порвала бы
--- её, а рядом с записью соседнего файбера показала бы ему недостроенный
--- спейс. Поэтому заводит ящик вывоз — `outbox.start()` при подъёме узла,
--- — а запись требует готового.
local function opened()
    if space_of.get() == nil then
        error('ящик ещё не заведён: позовите outbox.start() при подъёме узла', 3)
    end
end

--- Пишет событие в ящик и отдаёт его опознаватель.
---
--- В транзакции вызывающего — вставкой её же: откат уносит и событие.
--- Вне транзакции — своей: так ящик служит и надёжной отправкой без
--- данных, которая переживает перезапуск.
---@param name string Имя назначения: его отправитель назван в `configure`
---@param body any Тело — простые данные
---@param opts TntMessageSendOptions|nil Настройки отправки: уедут со строкой
---@return string id
function Module.write(name, body, opts)
    must.at(2).matches(name, 'имя назначения', NAME)

    local sender = shipper.sender(name)

    if sender == nil then
        local unknown = 'ящику некому отправлять «%s»: отправитель не назван'

        error(unknown:format(name), 2)
    end

    local given = common.options(opts, sender.features or {}, 2, WRITING)

    common.body(body, 2)

    local record = message.record(body, given)

    opened()

    local key = space_of.put(record.options.id, name, record, clock.realtime())

    commit.mark(key)

    counts.written = counts.written + 1

    return record.options.id
end

--- Запускает вывоз и заводит ящик, если узел пишет.
---
--- Первый проход идёт в файбере вызывающего, а не тактом: заведение
--- спейсов уступает, и запись, пришедшая в это окно, увидела бы ящик
--- недостроенным. На узле для чтения заводить нечего — ящик приедет
--- репликацией, а вывоз там молчит.
function Module.start()
    Module.flush()
    ticker:start()
end

--- Останавливает вывоз. Строки остаются на месте.
function Module.stop()
    ticker:stop()
end

--- Один проход вывоза прямо сейчас, не дожидаясь такта.
---@return table report
function Module.flush()
    return shipper.flush()
end

--- Зарытые строки: до `limit` от старых к новым.
---@param limit integer|nil Сколько взять; по умолчанию сто
---@return table[]
function Module.dead(limit)
    local caller = must.at(2)
    local asked = caller.optional.integer(limit, 'сколько взять') or Module.DEFAULT_DEAD

    caller.positive(asked, 'сколько взять')

    return space_of.dead(asked)
end

--- Возвращает до `count` зарытых на отправку и отдаёт число возвращённых.
---
--- Берутся те же строки, что отдал бы `dead(count)`, и ложатся в хвост
--- ящика одной транзакцией: новый ключ, тот же опознаватель, то же тело
--- и те же настройки отправки.
---
--- Отметка фиксации ставится, как у записи: без неё вывоз простоял бы
--- на возвращённой строке пять секунд и счёл бы её вставкой мимо ящика.
---
--- Пары у возврата нет, как и у записи: возврат — тоже запись, и отказ
--- box на узле для чтения приходит исключением, а зарытые остаются
--- на месте.
---@param count integer|nil Сколько вернуть; по умолчанию одну
---@return integer moved
function Module.kick(count)
    local caller = must.at(2)
    local asked = caller.optional.integer(count, 'сколько вернуть') or Module.DEFAULT_KICK

    caller.between(asked, 'сколько вернуть', 1, Module.MAX_KICK)

    if commit.in_txn() then
        error(FOREIGN, 2)
    end

    opened()

    local keys = space_of.revive(asked, clock.realtime())

    -- Вне транзакции отметка ложится сразу: возврат уже зафиксирован.
    for _, key in ipairs(keys) do
        commit.mark(key)
    end

    counts.kicked = counts.kicked + #keys

    return #keys
end

--- Что с ящиком сейчас: вывоз, глубина, возраст головы, счётчики.
---@return table
function Module.status()
    local shown = shipper.status()

    shown.running = ticker:running()
    shown.interval = settings.interval
    shown.counts.written = counts.written
    shown.counts.kicked = counts.kicked
    shown.senders = {}

    for name in pairs(settings.senders) do
        shown.senders[#shown.senders + 1] = name
    end

    -- По порядку: `pairs` от запуска к запуску разный, и сводка узла
    -- выходила бы каждый раз иной.
    table.sort(shown.senders)

    return shown
end

--- Возвращает ящик в исходное: вывоз остановлен, настройки, счёт и отметки
--- забыты. Строки в спейсах остаются — их стирает тот, кто заводил узел.
function Module.reset()
    ticker:stop()
    ticker:set_interval(Module.DEFAULT_INTERVAL)

    settings = defaults()
    counts = zeroed()

    shipper.reset()
    commit.reset()
end

settings = defaults()

ticker = loop.new({
    name = 'outbox_relay',
    interval = Module.DEFAULT_INTERVAL,
    tick = function()
        -- Полный кусок значит, что в ящике осталось ещё: следующий проход
        -- идёт без паузы, иначе миллион строк уезжал бы куском в секунду.
        if Module.flush().more then
            ticker:wake()
        end
    end,
    on_error = function(err)
        log.warn('такт вывоза ящика не отработал', { err = err })
    end,
})

-- Отметка фиксации будит вывоз: событие уходит сразу за транзакцией,
-- а не в конце такта.
commit.on_mark(function()
    ticker:wake()
end)

return Module
