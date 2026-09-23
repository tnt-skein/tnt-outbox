--- Вывоз ящика: проход по строкам с начала, отправка назначенным
--- отправителем, удаление отправленного.
---
--- **Выборка каждый раз с начала**, а не «после последнего отправленного
--- ключа»: под MVCC транзакция, взявшая ключ раньше, фиксируется позже
--- соседа с большим ключом, и курсор по ключу прошёл бы мимо неё навсегда.
---
--- **Строка, о фиксации которой ящик не знает, останавливает проход**:
--- порядок ключей важнее скорости. Строка без отметки выше границы
--- поколения — вставка мимо `outbox.write`; вывоз стоит на ней не дольше
--- `STALE` и открывает поколение заново.
---
--- **Повторяемый отказ кончает проход, а строка остаётся**: голова
--- ящика ждёт брокера сколько угодно, и порядок держится. Отступ перед
--- следующей попыткой растёт степенью — по `tnt-retry`, — иначе вывоз
--- ходил бы к лежащему брокеру каждый такт. Неповторяемый отказ уводит
--- строку в зарытые, и вывоз идёт дальше: одна негодная строка не вправе
--- остановить все события навсегда.
---
--- **Вывозит только узел для записи**. Прежний ведущий, став
--- репликой, перестаёт вывозить на первом же такте; новый открывает
--- поколение и вывозит с начала. Строка, отправленная прежним и не
--- удалённая до смены, уйдёт второй раз с тем же опознавателем — это
--- и есть «хотя бы раз», и повтор отсекает получатель по `id`.
---
--- **Счётчики вывоза растут вместе с рядами** (`tnt.outbox.series`),
--- а шкалы ряда берут глубину и возраст головы тем же замером, что
--- и сводка `status()`: разойтись им нечем.

local clock = require('tnt.clock')
local fail = require('tnt.must.fail')
local external = require('tnt.external')

local backoff = require('tnt.retry.backoff')

local commit = require('tnt.outbox.commit')
local series = require('tnt.outbox.series')
local space_of = require('tnt.outbox.space')

local log = require('tnt.log').new('tnt.outbox')

--- Беда, которая держится, иначе писала бы строку на каждый такт.
local repeated = require('tnt.log').changes('tnt.outbox')

local Module = {}

--- Сколько строк вывозится за один проход.
---
--- Проход ограничен не ради скорости, а ради такта: ящик бывает длиной
--- в миллионы строк, и первый же проход после долгого простоя занял бы
--- файбер на всё это время.
Module.DEFAULT_BATCH = 100

--- Сколько вывоз ждёт строку без отметки фиксации, прежде чем открыть
--- поколение заново, секунды.
Module.STALE = 5

--- Отступ после повторяемого отказа: степень с разбросом по `tnt-retry`.
---
--- Разброс здесь не против лавины — вывоз на узле один, — а против
--- совпадения тактов соседей, поднятых из одного образа; половина паузы
--- при этом гарантирована, и на лежащем брокере вывоз не крутится.
Module.DEFAULT_BACKOFF = { base = 1, factor = 2, jitter = 0.5, max = 60 }

--- Чего вывоз ждёт у головы ящика, пока о её фиксации не известно.
local UNCONFIRMED = 'о фиксации строки ещё не известно'

--- Итог отправки одной строки: ушла.
local SENT = 'sent'

--- Итог: неповторяемый отказ — строку в зарытые.
local DEAD = 'dead'

--- Итог: повторяемый отказ — строка на месте, проход кончается.
local HOLD = 'hold'

--- Внешние средства: спейсы ящика, часы и режим узла.
local source = external.install(Module, {
    space = space_of,
    now = clock.scheduler_now,
    realtime = clock.realtime,

    -- До `box.cfg` узла нет вовсе, и вывозить неоткуда: `box.cfg` ещё
    -- функция, а не таблица настроек.
    read_only = function()
        return type(box.cfg) == 'function' or box.info.ro == true
    end,
})

--- Настройки вывоза: отправители по именам, кусок прохода, отступ.
---@type table
local settings = { senders = {}, batch = Module.DEFAULT_BATCH, backoff = Module.DEFAULT_BACKOFF }

--- Счётчики вывоза с нуля.
---@return table<string, integer>
local function zeroed()
    return { sent = 0, dead = 0, failures = 0, stale = 0 }
end

--- Счётчики вывоза.
local counts = zeroed()

---@class TntOutboxRelayState Что вывоз помнит между проходами
---@field generation number|nil Когда открыли поколение; пустота — не открывали
---@field failures integer Сколько повторяемых отказов подряд: по ним отступ
---@field resume_at number|nil Когда вернуться к работе, по времени планировщика
---@field stalled { key: integer, since: number }|nil Строка без отметки и с какого мига

--- Состояние вывоза с чистого листа.
---
--- Одним местом, а не двумя: запуск и `reset` ставят одно и то же, а два
--- перечисления однажды разошлись бы — и вывоз после перезапуска вёл бы
--- себя иначе, чем после сброса.
---@return TntOutboxRelayState
local function unstarted()
    return { generation = nil, failures = 0, resume_at = nil, stalled = nil }
end

--- Состояние вывоза.
local state = unstarted()

--- Текст причины: у таблицы с полем `message` — оно, у пустоты — своё
--- слово.
---
--- Отказ без слов («отправитель вернул `nil, nil`») в зарытых выглядел бы
--- строкой «nil», и расследовать его было бы нечем.
---@param reason any
---@return string
local function text_of(reason)
    if type(reason) == 'table' and type(reason.message) == 'string' then
        return reason.message
    end

    if reason == nil then
        return 'отправитель отказал и причины не назвал'
    end

    return tostring(reason)
end

--- Открывает поколение: старший ключ читается до барьера, и строки
--- не старше его, пережившие запись барьера, зафиксированы.
local function open_generation()
    local top = source().space.top()
    local at = source().realtime()

    source().space.barrier(at)
    commit.open(top)

    state.generation = at
end

--- Чего вывоз ждёт у этой строки; пустота — можно отправлять.
---
--- Ответом служит причина, а не «нет»: по ней видно в журнале, почему
--- голова ящика стоит, и её же возвращает ожидание строки, легшей мимо
--- записи.
---@param row table
---@return string|nil
local function waiting(row)
    if commit.committed(row.key) then
        state.stalled = nil

        return nil
    end

    local now = source().now()
    local stalled = state.stalled

    if stalled == nil or stalled.key ~= row.key then
        state.stalled = { key = row.key, since = now }

        return UNCONFIRMED
    end

    if now - stalled.since < Module.STALE then
        return UNCONFIRMED
    end

    -- Строка легла в ящик мимо `outbox.write`: отметки ей никто не ставил,
    -- и ждать её фиксации можно вечно. Новая граница — старший ключ ящика,
    -- а он не меньше этой строки: после открытия поколения она под
    -- границей, и вывоз идёт дальше.
    counts.stale = counts.stale + 1
    state.stalled = nil
    log.warn(
        'строка ящика легла мимо outbox.write: вывоз по границе',
        { key = row.key }
    )
    open_generation()

    return nil
end

--- Зовёт отправителя имени и судит, что делать со строкой.
---@param row table
---@return string verdict
---@return string|nil reason
local function deliver(row)
    local sender = settings.senders[row.name]

    if sender == nil then
        -- Зарыть нельзя: строку ждёт настройка, а не человек, и отправитель
        -- имени бывает назван позже — при перечитывании конфигурации.
        return HOLD, ('отправителя «%s» ящику не назначили'):format(row.name)
    end

    local message = row.message
    local called, sent, err = pcall(sender.send, row.name, message.body, message.options)

    if not called then
        -- Бросок отправителя вывоз не роняет: строка остаётся до следующего
        -- прохода, как при любом повторяемом отказе.
        return HOLD, text_of(sent)
    end

    if sent ~= nil then
        return SENT
    end

    if type(err) == 'table' and err.retriable == false then
        return DEAD, text_of(err)
    end

    return HOLD, text_of(err)
end

--- Откладывает следующий проход: отступ растёт с числом отказов подряд.
---@param reason string
local function hold_off(reason)
    state.failures = state.failures + 1
    counts.failures = counts.failures + 1

    local pause = backoff.delay_for(state.failures, settings.backoff)

    state.resume_at = source().now() + pause
    repeated.warn(
        'вывоз ящика отложен: отказ отправителя',
        { err = reason, pause = pause }
    )
end

--- Настраивает вывоз. Настройки приходят проверенными от фасада.
---@param opts table
function Module.configure(opts)
    settings = opts
end

--- Отправитель этого имени либо пустота.
---@param name string
---@return table|nil
function Module.sender(name)
    return settings.senders[name]
end

--- Проход, идущий прямо сейчас: второй отправил бы ту же строку второй раз.
local busy = false

--- Один проход вывоза.
---@return table report
local function pass()
    local report = { sent = 0, dead = 0, more = false }

    if source().read_only() then
        -- На реплике такт молчит, а поколение откроется заново, когда узел
        -- вернётся в запись: отметки прежнего ведущего о чужих строках
        -- ничего не говорят.
        state.generation = nil
        report.skipped = 'узел только для чтения'

        return report
    end

    if state.resume_at ~= nil and source().now() < state.resume_at then
        report.skipped = 'отступ после отказа'

        return report
    end

    source().space.open()

    if state.generation == nil then
        open_generation()
    end

    local rows = source().space.head(settings.batch)

    for _, row in ipairs(rows) do
        local awaited = waiting(row)

        if awaited ~= nil then
            report.stalled = row.key
            log.debug('вывоз ждёт голову ящика', { key = row.key, err = awaited })

            return report
        end

        local verdict, reason = deliver(row)

        if verdict == HOLD then
            hold_off(reason --[[@as string]])
            report.err = reason

            return report
        end

        -- `elseif`, а не `else`: приговора четвёртого рода не бывает,
        -- а заведись он — строка останется на месте, и её увидит
        -- следующий проход, вместо того чтобы уехать молча.
        if verdict == DEAD then
            source().space.bury(row, reason --[[@as string]], source().realtime())
            series.count(counts, row.name, DEAD)
            report.dead = report.dead + 1
            log.error('строка ящика зарыта', { id = row.id, destination = row.name, err = reason })
        elseif verdict == SENT then
            source().space.remove(row.key)
            series.count(counts, row.name, SENT)
            report.sent = report.sent + 1
        end

        commit.forget(row.key)
    end

    state.failures = 0
    state.resume_at = nil

    -- Ровно кусок: больше `batch` выборка не отдаёт, и полный кусок значит,
    -- что в ящике осталось ещё.
    report.more = #rows == settings.batch

    return report
end

--- Один проход вывоза.
---
--- Отчёт говорит, что случилось: сколько ушло, сколько зарыто, почему
--- проход кончился раньше времени и остались ли строки на следующий.
---
--- Два прохода разом не идут: такт и позванный руками `flush` легко
--- приходятся друг на друга, а строку, которую один уже отправил и ещё
--- не удалил, второй отправил бы второй раз.
---@return { sent: integer, dead: integer, skipped: string|nil, err: string|nil, stalled: integer|nil, more: boolean }
function Module.flush()
    if busy then
        return { sent = 0, dead = 0, more = false, skipped = 'проход уже идёт' }
    end

    busy = true

    local ok, report = pcall(pass)

    busy = false

    if not ok then
        -- Место броска не своё: сорвался спейс, и показывать на строку
        -- вывоза вызывающему незачем.
        fail.raise(report)
    end

    return report
end

--- Глубина ящика и возраст головы; пустота — ящик не заведён.
---
--- Один замер на сводку и на шкалы ряда: два подсчёта однажды разошлись бы.
---@return TntOutboxDepth|nil
local function measured()
    local depth = source().space.depth()

    if depth == nil then
        return nil
    end

    local head = source().space.first()

    depth.oldest_seconds = head ~= nil and source().realtime() - head.created or nil

    return depth
end

series.watch(measured)

--- Что с вывозом сейчас: счётчики, глубина, возраст головы, отступ.
---
--- Возраст головы — главный ряд тревоги: вывоз, стоящий на отказе брокера,
--- иначе не виден.
---@return table
function Module.status()
    local counted = {}

    for name, value in pairs(counts) do
        counted[name] = value
    end

    local depth = measured() or { pending = 0, dead = 0 }

    return {
        batch = settings.batch,
        counts = counted,
        pending = depth.pending,
        dead = depth.dead,
        oldest_seconds = depth.oldest_seconds,
        boundary = commit.boundary(),
        generation = state.generation,
        holding = state.resume_at ~= nil,
    }
end

--- Забывает счёт, отступ и поколение. Нужен проверкам и смене ведущего.
function Module.reset()
    busy = false
    settings = { senders = {}, batch = Module.DEFAULT_BATCH, backoff = Module.DEFAULT_BACKOFF }
    counts = zeroed()
    state = unstarted()
end

return Module
