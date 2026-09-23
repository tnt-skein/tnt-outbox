--- Общие средства проверок ящика исходящих.
---
--- Ящик стоит на `box`, и его спейсы, отметки фиксации и барьер поколения
--- проверяются на настоящем узле (`outbox_node_test.lua`, `sync_node_test.lua`):
--- видимость незафиксированной строки, последовательность ключей и триггеры
--- фиксации двойником не покажешь. Там же — отметки получателя
--- (`inbox_node_test.lua`): отметка в транзакции обработчика и её откат,
--- уборка по сроку. В процессе проверок остаётся то, у чего своя работа:
--- настройки и тело записи, отметки за внешней зависимостью, проход вывоза
--- на двойнике спейсов и проверки аргументов отметок.
---
--- Исходники читаются с диска, а не через `require`: у Tarantool свой
--- загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt.message`, `tnt.must`, `tnt.clock`, `tnt.context`, `tnt.id`,
--- `tnt.log`, `tnt.loop`, `tnt.retry`, `tnt.external` — берутся из `.rocks` обычным
--- `require`: проверяется этот пакет, а не они. На временном узле они
--- берутся так же. Общий способ объявить ряд `tnt.metrics.series`
--- — тоже из `.rocks`, но файлами (`ROWS`).
---
--- Оснастка в `test/testing/` — загрузчик исходников, ловушка журнала,
--- запись файлов и временный узел — грузится так же, файлами, и один раз
--- на процесс: второй экземпляр загрузчика не знал бы, что вытеснил первый,
--- и не вернул бы вытесненное на место.
---
--- Проверки берут всё через этот помощник, а не из оснастки напрямую:
--- помощник — единственное, чем файл проверок отличается от того же файла
--- в наборе, где пакет живёт рядом со своими зависимостями.

local fio = require('fio')
local t = require('luatest')

--- Реестр встроенного metrics: его методов в аннотациях ядра нет.
---@type any
local registry = require('metrics')

--- Модули оснастки в порядке зависимостей: узел берёт файлы и загрузчик,
--- ловушка журнала — загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.files', path = 'test/testing/files.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
    { name = 'tnt.testing.node', path = 'test/testing/node.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

--- Оснастка проверок под теми именами, что зовёт помощник.
local testing = {
    load_sources = package.loaded['tnt.testing.sources'].load,
    module = package.loaded['tnt.testing.sources'].module,
    capture_log = package.loaded['tnt.testing.journal'].capture,
    start_node = package.loaded['tnt.testing.node'].start,
    stop_node = package.loaded['tnt.testing.node'].stop,
}

--- Общий способ объявить ряд — файлами из `.rocks`, свой экземпляр
--- на каждую загрузку помощника, а не `require`.
---
--- Ряды — состояние процесса: повторное объявление отдаёт прежний ряд
--- и ставит шкале источник последней загрузки. Пакет ставит свои
--- источники шкал при загрузке, и с одним экземпляром на процесс шкалы
--- собирал бы пакет последнего загруженного файла проверок, а не тот, что
--- под проверкой. Со своим экземпляром ряды каждой загрузки возвращаются
--- в реестр первым же наблюдением.
local ROWS = {
    { name = 'tnt.metrics.series.labels', path = '.rocks/share/tarantool/tnt/metrics/series/labels.lua' },
    { name = 'tnt.metrics.series.collector', path = '.rocks/share/tarantool/tnt/metrics/series/collector.lua' },
    { name = 'tnt.metrics.series', path = '.rocks/share/tarantool/tnt/metrics/series.lua' },
}

--- Модули в порядке зависимостей: ряды, пакет и двойник отправителя — он
--- нужен и в процессе проверок, и на узле.
local OWN = {
    ROWS[1],
    ROWS[2],
    ROWS[3],
    { name = 'tnt.outbox.message', path = 'tnt/outbox/message.lua' },
    { name = 'tnt.outbox.commit', path = 'tnt/outbox/commit.lua' },
    { name = 'tnt.outbox.space', path = 'tnt/outbox/space.lua' },
    { name = 'tnt.outbox.series', path = 'tnt/outbox/series.lua' },
    { name = 'tnt.outbox.shipper', path = 'tnt/outbox/shipper.lua' },
    { name = 'tnt.outbox.inbox', path = 'tnt/outbox/inbox.lua' },
    { name = 'tnt.outbox', path = 'tnt/outbox.lua' },
    { name = 'tnt.outbox.recorder', path = 'test/recorder.lua' },
}

--- Средства проверок ящика.
---@class TntOutboxTestHelper
---@field outbox table Фасад ящика
---@field message table Строка записи
---@field commit table Отметки фиксации
---@field space table Спейсы ящика
---@field shipper table Вывоз
---@field series table Ряды ящика
---@field inbox table Отметки получателя
---@field recorder table Двойник отправителя
---@field context table Контекст файбера
local helper = { MODULES = OWN }

--- Фасад пакета из исходников: в процессе проверок грузится один раз,
--- а состояние ящика проверки возвращают сами (`forget`).
helper.outbox = testing.load_sources(helper.MODULES, 'tnt.outbox')

--- Тело: цепочка из `levels` вложенных таблиц под корнем — всего таблиц
--- на одну больше.
---@param levels integer
---@return table
function helper.deep(levels)
    local root = {}
    local node = root

    for _ = 1, levels do
        node.next = {}
        node = node.next
    end

    return root
end

--- Тело: список из `count` чисел — значений в нём на одно больше, считая
--- сам список.
---@param count integer
---@return table
function helper.wide(count)
    local list = {}

    for index = 1, count do
        list[index] = index
    end

    return list
end

-- Части пакета берутся из той же загрузки, что и фасад: взятые `require`,
-- они пришли бы установленной копией из `.rocks`.
helper.message = testing.module('tnt.outbox.message')
helper.commit = testing.module('tnt.outbox.commit')
helper.space = testing.module('tnt.outbox.space')
helper.shipper = testing.module('tnt.outbox.shipper')
helper.series = testing.module('tnt.outbox.series')
helper.inbox = testing.module('tnt.outbox.inbox')
helper.recorder = testing.module('tnt.outbox.recorder')
helper.context = require('tnt.context')

--- Сверяет, что каждый вызов бросает названный отказ и винит строку
--- вызова в файле проверок, а не внутри пакета.
---
--- Вызов стоит в замыкании первой строкой тела, то есть строкой ниже
--- слова `function`: место броска сверяется с ней целиком — файлом,
--- строкой и текстом.
---@param cases table[] Пары: замыкание с вызовом и текст броска
function helper.assert_blamed(cases)
    for _, case in ipairs(cases) do
        local _, err = pcall(case[1])
        local info = debug.getinfo(case[1], 'S') --[[@as { short_src: string, linedefined: integer }]]

        t.assert_equals(err, ('%s:%d: %s'):format(info.short_src, info.linedefined + 1, case[2]))
    end
end

--- Значение мимо проверки типов: негодный аргумент нарочно.
---@param value any
---@return any
function helper.wrong(value)
    return value
end

--- Совпадают ли метки: одинаковый набор имён с одинаковыми значениями.
---@param left table
---@param right table
---@return boolean
local function same_labels(left, right)
    for name, value in pairs(left) do
        if right[name] ~= value then
            return false
        end
    end

    for name in pairs(right) do
        if left[name] == nil then
            return false
        end
    end

    return true
end

--- Наблюдения реестра по имени ряда — после источников шкал, как их
--- собирает выкладка.
---@param name string
---@return table[]
function helper.samples(name)
    local found = {}

    for _, observation in ipairs(registry.collect({ invoke_callbacks = true })) do
        if observation.metric_name == name then
            table.insert(found, observation)
        end
    end

    return found
end

--- Что увидит сборщик: число ряда с ровно такими метками.
---@param name string
---@param labels table|nil
---@return number|nil
function helper.value(name, labels)
    for _, observation in ipairs(helper.samples(name)) do
        if same_labels(observation.label_pairs, labels or {}) then
            return observation.value
        end
    end

    return nil
end

--- Ловушка журнала: записи о зарытом, отложенном вывозе и строке мимо
--- записи видны только ею.
helper.journal = testing.capture_log()

--- Возвращает ящик в исходное: вывоз и уборка остановлены, настройки
--- и счёт забыты, отметки фиксации сняты, внешние зависимости настоящие,
--- журнал забыт.
function helper.forget()
    helper.outbox.reset()
    helper.inbox.reset()
    helper.commit._set_source(nil)
    helper.shipper._set_source(nil)
    helper.inbox._set_source(nil)
    helper.journal.forget()
end

--- Отправитель-двойник. Без названных признаков умеет всё: пустая
--- таблица признаков — это «запретов нет».
---@param answers table[]|nil
---@param features table<string, boolean>|nil
---@return table
function helper.sender(answers, features)
    return helper.recorder.new(answers, features)
end

--- Двойник спейсов ящика: строки в списке по порядку ключей.
---
--- Спейсов у ящика три, а вывозу от них нужно немногое — начало, удаление,
--- зарытие и барьер. Двойник даёт это без узла, и проход вывоза проверяется
--- там же, где живут его решения.
---@param rows table[]|nil Строки ящика по порядку
---@return table
function helper.spaces(rows)
    local double = { rows = rows or {}, dead = {}, barriers = 0, opened = 0 }

    function double.open()
        double.opened = double.opened + 1
    end

    function double.head(limit)
        local head = {}

        for index = 1, math.min(limit, #double.rows) do
            head[index] = double.rows[index]
        end

        return head
    end

    function double.first()
        return double.rows[1]
    end

    function double.top()
        local last = double.rows[#double.rows]

        return last ~= nil and last.key or 0
    end

    function double.barrier()
        double.barriers = double.barriers + 1
    end

    function double.remove(key)
        for index, row in ipairs(double.rows) do
            if row.key == key then
                table.remove(double.rows, index)

                return
            end
        end
    end

    function double.bury(row, reason, at)
        table.insert(double.dead, { id = row.id, reason = reason, buried = at, row = row })
        double.remove(row.key)
    end

    function double.depth()
        return { pending = #double.rows, dead = #double.dead }
    end

    return double
end

--- Строка ящика для двойника.
---
--- Опознаватель есть всегда, как у настоящей записи: по нему видно, что
--- вывоз отправляет сохранённое, а не заводит своё.
---@param key integer
---@param name string
---@param body any
---@param options table|nil
---@return table
function helper.row(key, name, body, options)
    local given = options or {}

    given.id = given.id or ('id-%d'):format(key)

    return {
        key = key,
        id = given.id,
        name = name,
        message = { body = body, options = given },
        created = 1000 + key,
    }
end

--- Поднимает узел с исходниками пакета и двойником отправителя: настоящие
--- спейсы, последовательность и триггеры фиксации. Узел проверка обязана
--- остановить сама — `stop_node`.
---@param alias string Имя узла в артефактах прогона
---@return table server
function helper.start_node(alias)
    return testing.start_node({ alias = alias, modules = helper.MODULES })
end

--- Останавливает узел и убирает его каталог.
helper.stop_node = testing.stop_node

--- Ждёт, пока условие сбудется; иначе валит проверку с поясняющим текстом.
---@param about string Чего ждали
---@param predicate fun(): boolean
function helper.until_true(about, predicate)
    t.helpers.retrying({ timeout = 5, delay = 0.01 }, function()
        assert(predicate(), about)
    end)
end

return helper
