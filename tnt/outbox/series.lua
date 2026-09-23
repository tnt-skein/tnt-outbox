--- Ряды метрик ящика: сколько строк ждёт вывоза и сколько зарыто,
--- сколько ждёт голова, сколько ушло и сколько зарыто по назначению.
---
--- **Ряды свои, `outbox_*`, а не общие ряды сообщений.** Отправляет
--- из ящика отправитель — очередь, клиент брокера, — и он сам считает
--- отправленное в рядах `message_*` под тем же именем назначения: ящик,
--- считающий туда же, сложил бы каждое событие дважды. Своё у ящика —
--- то, чего отправитель не видит: строки, которые ещё не ушли.
---
--- **Возраст головы — главный ряд тревоги.** Повторяемый отказ держит
--- голову ящика, и все события за ней стоят; вывоз, стоящий на отказе
--- брокера, иначе не виден. У пустого ящика голова ждёт ноль секунд,
--- а не «ничего»: правило тревоги на пропавшем ряде молчит, и пустой
--- ящик не должен выглядеть как узел, который перестали собирать.
---
--- **Шкалы собираются перед выкладкой**: глубина — это длина спейса,
--- и считать её на каждой строке незачем. Узел, где ящик не заведён,
--- строк шкал не показывает вовсе: ящика там нет. Реплика показывает
--- то же, что ведущий, — ящик реплицируется вместе с данными, и голова,
--- которую никто не вывозит, стареет и там.

local metrics = require('tnt.metrics.series')

local Module = {}

--- Сколько разных назначений видно в рядах. Назначения называет
--- настройка ящика, а не данные, и больше сотни — это уже ошибка;
--- сверх потолка назначение идёт словом `_other`.
Module.DESTINATIONS = 100

---@class TntOutboxDepth Глубина ящика и возраст головы
---@field pending integer Сколько строк ждёт вывоза
---@field dead integer Сколько строк в зарытых
---@field oldest_seconds number|nil Возраст головы по стенным часам; пустота — ящик пуст

--- Источник шкал: его ставит вывоз. До того источник молчит — ящика
--- нет, и строк у шкал нет тоже.
---@type fun(): TntOutboxDepth|nil
local measure = function() end

local sent = metrics.counter('outbox_sent_total', {
    help = 'Сколько строк ящика отправлено и удалено: по назначению',
    labels = { destination = Module.DESTINATIONS },
})

local dead = metrics.counter('outbox_dead_total', {
    help = 'Сколько строк ящика зарыто неповторяемым отказом отправителя: по назначению',
    labels = { destination = Module.DESTINATIONS },
})

metrics.gauge('outbox_depth', {
    help = 'Сколько строк лежит в ящике: ждёт вывоза и зарыто',
    labels = { state = { 'pending', 'dead' } },
    collect = function(gauge)
        local depth = measure()

        if depth ~= nil then
            gauge:set(depth.pending, { state = 'pending' })
            gauge:set(depth.dead, { state = 'dead' })
        end
    end,
})

metrics.gauge('outbox_oldest_seconds', {
    help = 'Сколько секунд голова ящика ждёт вывоза; у пустого ящика — ноль',
    collect = function(gauge)
        local depth = measure()

        if depth ~= nil then
            local oldest = depth.oldest_seconds

            -- Часы узлов расходятся: строку, записанную прежним ведущим,
            -- новый видит из будущего, и отрицательный возраст — это ноль.
            -- Головы нет — ящик пуст, и ждёт она ноль секунд.
            gauge:set(oldest ~= nil and math.max(oldest, 0) or 0)
        end
    end,
})

--- Какой ряд растёт с каким счётчиком вывоза.
---@type table<string, TntMetricsSeries>
local COUNTERS = { sent = sent, dead = dead }

--- Считает строку вывоза: и в счётчике `status()`, и в ряду.
---@param counts table<string, integer> Счётчики вывоза
---@param name string Имя назначения строки
---@param what string `sent` — ушла, `dead` — зарыта
function Module.count(counts, name, what)
    counts[what] = counts[what] + 1
    COUNTERS[what]:inc(1, { destination = name })
end

--- Ставит источник шкал: глубину ящика и возраст головы.
---@param source fun(): TntOutboxDepth|nil Пустота — ящик не заведён
function Module.watch(source)
    measure = source
end

return Module
