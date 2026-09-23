--- Отметки получателя: повтор доставки отсекается отметкой опознавателя
--- в той же транзакции, что и запись обработчика (`docs/outbox.md`,
--- «Отметки получателя»).
---
---     local inbox = require('tnt.outbox.inbox')
---
---     inbox.start()   -- при подъёме узла: спейс заведён, уборка пошла
---
---     box.atomic(function()
---         if inbox.claim('receipts', message) then
---             box.space.receipts:insert({ message.id, message.body.order })
---         end
---     end)
---
--- **Повтор приходит у всех долговечных очередей**: их гарантия — хотя бы
--- раз, и сообщение, обработанное перед обрывом, приходит снова с тем же
--- `id`. Отсечь его может только получатель: очередь не знает, успел ли
--- обработчик записать своё.
---
--- **Отметка — вставка в транзакцию обработчика.** Фиксация оставляет
--- и запись, и отметку, откат снимает обе, и повтор после отката
--- обрабатывается заново, как и должен. Вне транзакции отметка —
--- исключение: поставленная отдельно от записи, она либо переживёт
--- несделанную запись и проглотит повтор, либо не успеет и ничего
--- не отсечёт.
---
--- **Ключ — получатель и опознаватель**: одно событие обрабатывают
--- несколько получателей, и отметка одного не вправе отсечь другого.
---
--- **Две доставки разом** отсекает сам box. Без MVCC уступки внутри
--- транзакции нет, и вторая видит отметку первой уже в миг вставки,
--- до записи в WAL; откатится первая — её доставка откажет и придёт снова,
--- как после всякого отказа обработчика. Под MVCC вторая может отметки
--- первой не увидеть, но тогда её откатывает конфликт фиксации.
---
--- **Отметки живут не вечно**: уборка на `tnt-loop` стирает те, что старше
--- срока хранения. Повтор, пришедший после уборки своей отметки,
--- обработается второй раз, поэтому срок — не меньше самого длинного окна
--- повтора у очередей, из которых получатель берёт сообщения.
---
--- **Уборка — только на узле для записи**, кусками по `batch`, кусок —
--- одной транзакцией, с уступкой перед каждой выборкой: обход без уступки
--- на десятках тысяч отметок упёрся бы в срез файбера, и ядро оборвало бы
--- фоновый файбер уборки. Курсора нет: стёртое не возвращается, и каждый
--- кусок — снова то, что старше срока. Обход не снимок, но отметка,
--- поставленная за уступкой, моложе срока, и в кусок она не попадает.
--- Ожидания после смены ведущего нет: уборка стирает только старое,
--- и стирание, пришедшее хвостом прежнего ведущего, стирает то же самое.
---
--- **Спейс заводит `start`**, а не отметка: заведение — DDL, оно уступает
--- и в транзакции обработчика порвало бы её.

local fiber = require('fiber')

local clock = require('tnt.clock')
local external = require('tnt.external')
local loop = require('tnt.loop')
local must = require('tnt.must')

local log = require('tnt.log').new('tnt.outbox')

---@class TntOutboxInbox
---@field _set_source fun(replacement: table|nil) Подмена средств — для проверок; ставит её `external.install`
local Module = {}

--- Имя спейса отметок. Впереди имя пакета, как у спейсов ящика: спейс
--- `inbox` у приложения бывает своим.
Module.NAME = 'outbox_inbox'

--- Сколько хранить отметку, секунды: неделя.
---
--- Столько по умолчанию Kafka хранит тему и смещения группы: группа,
--- простоявшая до недели, перечитывает от своего смещения то, что успела
--- обработать и не успела подтвердить, — это самое длинное окно повтора
--- из тех, что стоят без настройки. Очередь в спейсе и брокер AMQP
--- повторяют обычно через минуты — после срока взятия, обрыва канала или
--- перезапуска. Цена — память: отметка занимает около ста десяти байт,
--- и неделя по десять сообщений в секунду — около 660 МБ.
Module.DEFAULT_RETENTION = 7 * 86400

--- Как часто идёт уборка, секунды. Срок меряется сутками, и спешить ей
--- некуда; кусок, стёртый целиком, будит следующий сразу.
Module.DEFAULT_INTERVAL = 60

--- Сколько отметок стирается одной транзакцией: тысяча — одна запись
--- в WAL и около десятка миллисекунд без уступки.
Module.DEFAULT_BATCH = 1000

--- Имя получателя: буква, дальше буквы, цифры, подчёркивание, точка
--- и дефис — `receipts`, `billing.invoice-sent`.
local RECEIVER = '^%a[%w_.-]*$'

--- Настройки отметок.
local SETTINGS = { retention = '?number', interval = '?number', batch = '?integer' }

--- Строка отметки: получатель, опознаватель сообщения и стенное время
--- отметки — по нему уборка судит о сроке.
local ROW = {
    { name = 'receiver', type = 'string' },
    { name = 'id', type = 'string' },
    { name = 'marked', type = 'number' },
}

--- Отметка вне транзакции.
local OUTSIDE =
    'отметку ставят в транзакции обработчика: без его записи она повтор не отсекает'

--- Отметка до заведения спейса.
local UNOPENED =
    'отметки получателя ещё не заведены: позовите inbox.start() при подъёме узла'

--- Уборка внутри транзакции.
local INSIDE =
    'уборка отметок уступает и сама фиксирует куски: внутри транзакции её не зовут'

--- Внешние средства: транзакция вызывающего, режим узла и стенные часы.
local source = external.install(Module, {
    -- До `box.cfg` любой вопрос к box — исключение «Please call box.cfg{}
    -- first»: узла нет, значит нет и транзакции.
    in_txn = function()
        return type(box.cfg) ~= 'function' and box.is_in_txn()
    end,

    -- До `box.cfg` убирать негде: `box.cfg` ещё функция, а не таблица.
    read_only = function()
        return type(box.cfg) == 'function' or box.info.ro == true
    end,

    realtime = clock.realtime,
})

---@class TntOutboxInboxSettings
---@field retention number|nil Сколько хранить отметку, секунды
---@field interval number|nil Как часто идёт уборка, секунды
---@field batch integer|nil Сколько отметок стирается одной транзакцией

---@class TntOutboxInboxReport Итог прохода уборки
---@field removed integer Сколько отметок стёрто
---@field more boolean Кусок стёрт целиком: старое, может быть, ещё осталось
---@field skipped string|nil Почему проход не шёл

--- Умолчания настроек.
---@return { retention: number, interval: number, batch: integer }
local function defaults()
    return {
        retention = Module.DEFAULT_RETENTION,
        interval = Module.DEFAULT_INTERVAL,
        batch = Module.DEFAULT_BATCH,
    }
end

--- Счётчики с нуля.
---@return table<string, integer>
local function zeroed()
    return { claimed = 0, repeated = 0, removed = 0 }
end

--- Действующие настройки.
local settings = defaults()

--- Счётчики: отметки и повторы — в миг вызова, стёртое — по проходам.
local counts = zeroed()

--- Такт уборки. Собирается ниже, рядом с тем, что его запускает.
---@type any
local ticker

--- Спейс отметок либо пустота, пока он не заведён.
---
--- Заведённым считается спейс со вторым индексом: заведение уступает
--- между спейсом и индексами, а первичный заводится раньше второго.
---@return table|nil
local function ready()
    if type(box.cfg) == 'function' then
        return nil
    end

    local space = box.space[Module.NAME] --[[@as table|nil]]

    if space == nil or space.index.marked == nil then
        return nil
    end

    return space
end

--- Заводит спейс отметок, если его ещё нет.
---@return table
local function open()
    local space = box.schema.space.create(Module.NAME, { if_not_exists = true, format = ROW })

    space:create_index('primary', { if_not_exists = true, parts = { 'receiver', 'id' } })
    space:create_index('marked', { if_not_exists = true, parts = { 'marked' }, unique = false })

    return space
end

--- Бросает, если вызывающий в транзакции: уборка уступает, а уступка
--- рвёт транзакцию memtx, и узнал бы он об этом только на фиксации.
---@param level integer Уровень вины от этой функции
local function outside_txn(level)
    if source().in_txn() then
        error(INSIDE, level)
    end
end

--- Один проход уборки: стирает до `batch` отметок старше срока.
---@return TntOutboxInboxReport
local function pass()
    if source().read_only() then
        -- На реплике уборка молчит: стирание приедет от ведущего.
        return { removed = 0, more = false, skipped = 'узел только для чтения' }
    end

    local space = open()

    fiber.yield()

    -- Срок берётся после уступки: иначе горизонт отставал бы на неё.
    local horizon = source().realtime() - settings.retention
    local doomed = space.index.marked:select({ horizon }, { iterator = 'LT', limit = settings.batch })

    box.atomic(function()
        for _, tuple in ipairs(doomed) do
            space:delete({ tuple.receiver, tuple.id })
        end
    end)

    counts.removed = counts.removed + #doomed

    -- Больше `batch` выборка не отдаёт: полный кусок значит, что старое
    -- могло остаться.
    return { removed = #doomed, more = #doomed == settings.batch }
end

--- Настраивает срок хранения и уборку.
---
--- Настройки задаются разом: не названное теперь возвращается
--- к умолчанию, иначе сбросить заданное прежде было бы нечем.
---@param opts TntOutboxInboxSettings|nil
function Module.configure(opts)
    local caller = must.at(2)
    local given = caller.optional.options(opts, 'настройки отметок', SETTINGS) or {}

    caller.optional.positive(given.retention, 'настройки отметок.retention')
    caller.optional.positive(given.interval, 'настройки отметок.interval')
    caller.optional.positive(given.batch, 'настройки отметок.batch')

    local chosen = defaults()

    for name, value in pairs(given) do
        -- `box.NULL` из конфигурации значит «не задано», как и нет ключа:
        -- проверки его пропускают, а в условии он истинен и дошёл бы
        -- до счёта срока пустым cdata.
        if value ~= nil then
            chosen[name] = value
        end
    end

    settings = chosen
    ticker:set_interval(settings.interval)
end

--- Отмечает сообщение за получателем в транзакции его обработчика.
---
--- `true` — отметки не было, и обработчик делает своё; `false` — повтор,
--- делать нечего. Отметка фиксируется и откатывается вместе с транзакцией.
---@param receiver string Имя получателя: у каждого свои отметки
---@param message { id: string } Сообщение договора; нужен только `id`
---@return boolean first
function Module.claim(receiver, message)
    local caller = must.at(2)

    caller.matches(receiver, 'имя получателя', RECEIVER)
    caller.table(message, 'сообщение')
    caller.not_empty(message.id, 'сообщение.id')

    if not source().in_txn() then
        error(OUTSIDE, 2)
    end

    local space = ready()

    if space == nil then
        error(UNOPENED, 2)
    end

    if space:get({ receiver, message.id }) ~= nil then
        counts.repeated = counts.repeated + 1

        return false
    end

    space:insert({ receiver, message.id, source().realtime() })
    counts.claimed = counts.claimed + 1

    return true
end

--- Один проход уборки прямо сейчас, не дожидаясь такта.
---@return TntOutboxInboxReport
function Module.sweep()
    outside_txn(3)

    return pass()
end

--- Заводит спейс, если узел пишет, и запускает такт уборки.
---
--- Первый проход идёт в файбере вызывающего, а не тактом: он заводит
--- спейс, а заведение уступает, и отметка, пришедшая в это окно, увидела
--- бы спейс недостроенным. На узле для чтения заводить нечего — спейс
--- приедет репликацией.
function Module.start()
    outside_txn(3)
    pass()
    ticker:start()
end

--- Останавливает такт уборки. Отметки остаются.
function Module.stop()
    ticker:stop()
end

--- Что с отметками сейчас: настройки, сколько их, возраст старшей, счёт.
---
--- Возраст старшей — главный ряд тревоги: уборка, которая не идёт,
--- видна по нему раньше, чем по памяти.
---@return table
function Module.status()
    local space = ready()
    local oldest = space ~= nil and space.index.marked:min() or nil
    local counted = {}

    for name, value in pairs(counts) do
        counted[name] = value
    end

    return {
        running = ticker:running(),
        retention = settings.retention,
        interval = settings.interval,
        batch = settings.batch,
        marks = space ~= nil and space:len() or 0,
        oldest_seconds = oldest ~= nil and source().realtime() - oldest.marked or nil,
        counts = counted,
    }
end

--- Возвращает модуль в исходное: такт остановлен, настройки и счёт
--- забыты. Отметки в спейсе остаются — их стирает тот, кто заводил узел.
function Module.reset()
    ticker:stop()
    Module.configure()

    counts = zeroed()
end

ticker = loop.new({
    name = 'outbox_inbox',
    interval = Module.DEFAULT_INTERVAL,
    tick = function()
        -- Кусок, стёртый целиком, значит, что старого ещё много: следующий
        -- проход идёт без паузы, иначе миллион отметок уходил бы тысячей
        -- в минуту.
        if pass().more then
            ticker:wake()
        end
    end,
    on_error = function(err)
        log.warn('такт уборки отметок не отработал', { err = err })
    end,
})

return Module
