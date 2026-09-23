--- Спейсы ящика: сам ящик, зарытые и барьер поколения.
---
--- **Ключ строки — последовательность спейса**: она не начинается заново
--- ни после удаления всех строк, ни после `truncate`, ни после перезапуска.
--- На ключе стоит граница поколения — всё не старше её вывоз считает
--- зафиксированным, — и номер, начатый заново на опустевшем ящике, лёг бы
--- под старую границу: строку транзакции, ждущей кворума, вывоз отправил бы
--- как зафиксированную. Ключ держит и порядок вывоза; опознаватель
--- сообщения лежит в самой строке, и повторная отправка несёт тот же.
---
--- **Зарытое — в своём спейсе**, а не отметкой в ящике: строка ящика
--- уходит из вывоза целиком, вместе с причиной, и порядок остальных
--- от неё не зависит. Возврат кладёт её в ящик заново, в хвост.
---
--- **Барьер — строка в своём спейсе**, а не отметка в ящике: барьер
--- пишется на каждом открытии поколения, и в ящике он был бы строкой,
--- которую вывоз обязан отличать от сообщения.
---
--- Заведение спейсов — DDL, и оно **уступает**: между созданием спейса
--- и его первичным индексом другой файбер видит ящик недостроенным, а
--- вставка в него отказывает невнятно. Поэтому спейс считается заведённым
--- только вместе с индексом, а строит его один — вывоз (`outbox.start()`
--- при подъёме узла). Запись в чужой транзакции завести ящик и не могла
--- бы: уступка порвала бы транзакцию вызывающего.

local fiber = require('fiber')

local Module = {}

--- Имя спейса ящика.
Module.NAME = 'outbox'

--- Имя спейса зарытых.
Module.DEAD = 'outbox_dead'

--- Имя спейса барьера поколения.
Module.BARRIER = 'outbox_barrier'

--- Ключ единственной строки барьера.
Module.BARRIER_KEY = 1

--- Строка ящика: ключ последовательности, опознаватель, назначение,
--- тело с настройками отправки и стенное время записи.
local ROW = {
    { name = 'key', type = 'unsigned' },
    { name = 'id', type = 'string' },
    { name = 'name', type = 'string' },
    { name = 'message', type = 'map' },
    { name = 'created', type = 'number' },
}

--- Строка зарытых: опознаватель, причина, время и строка ящика целиком.
local DEAD_ROW = {
    { name = 'id', type = 'string' },
    { name = 'reason', type = 'string' },
    { name = 'buried', type = 'number' },
    { name = 'row', type = 'array' },
}

--- Строка барьера: один ключ и время записи.
local BARRIER_ROW = {
    { name = 'id', type = 'unsigned' },
    { name = 'written', type = 'number' },
}

--- Спейс по имени либо пустота, пока узла нет.
---@param name string
---@return table|nil
local function space_of(name)
    -- До `box.cfg` спейсов нет вовсе: `box.cfg` ещё функция, а не таблица.
    if type(box.cfg) == 'function' then
        return nil
    end

    return box.space[name]
end

--- Строка ящика таблицей: с ней работает вывоз, и двойник спейса в
--- проверках отдаёт такую же.
---@param tuple table
---@return table
local function row_of(tuple)
    return tuple:tomap({ names_only = true })
end

--- Спейс ящика либо пустота, если он ещё не заведён.
---
--- Спейс без первичного индекса — недостроенный: заведение уступает между
--- ним и индексом, и в это окно ящик виден, а писать в него нельзя.
---@return table|nil
function Module.get()
    local space = space_of(Module.NAME)

    if space == nil or space.index.primary == nil then
        return nil
    end

    return space
end

--- Спейс ящика, который обязан быть.
---
--- Заведение — DDL, и звать его на каждое движение нельзя: внутри чужой
--- транзакции DDL не идёт. Поэтому ящик заводит вывоз — `outbox.start()`
--- при подъёме узла, — а движения требуют готового.
---
--- Винится строка того, кто позвал движение: ящик без спейса — это
--- забытый `outbox.start()`, и искать его надо у вызывающего.
---@return table
local function required()
    local space = Module.get()

    if space == nil then
        error(('спейса %s нет: ящик заводит outbox.start()'):format(Module.NAME), 3)
    end

    return space
end

--- Заводит спейсы ящика, если их ещё нет.
function Module.open()
    local space = box.schema.space.create(Module.NAME, { if_not_exists = true, format = ROW })

    -- Аннотация плагина ждёт у `sequence` имя или номер, но `true` —
    -- законная просьба завести последовательность спейсу.
    ---@diagnostic disable-next-line: assign-type-mismatch
    space:create_index('primary', { if_not_exists = true, parts = { 'key' }, sequence = true })

    local dead = box.schema.space.create(Module.DEAD, { if_not_exists = true, format = DEAD_ROW })

    dead:create_index('primary', { if_not_exists = true, parts = { 'id' } })

    local barrier = box.schema.space.create(Module.BARRIER, { if_not_exists = true, format = BARRIER_ROW })

    barrier:create_index('primary', { if_not_exists = true, parts = { 'id' } })
end

--- Кладёт строку в ящик и отдаёт её ключ.
---
--- Ключ ставит последовательность спейса: `box.NULL` на его месте — просьба
--- взять следующий.
---@param id string Опознаватель сообщения
---@param name string Назначение
---@param record TntOutboxRecord Тело и настройки отправки
---@param created number Стенное время записи
---@return integer key
function Module.put(id, name, record, created)
    return required():insert({ box.NULL, id, name, record, created })[1]
end

--- Начало ящика: до `limit` строк по порядку ключей.
---
--- Уступка перед выборкой, а не после: обход без уступки на десятках тысяч
--- строк упирается в срез файбера, и фоновый файбер умирает молча.
---@param limit integer
---@return table[]
function Module.head(limit)
    fiber.yield()

    local rows = {}

    for _, tuple in ipairs(required():select({}, { limit = limit })) do
        rows[#rows + 1] = row_of(tuple)
    end

    return rows
end

--- Первая строка ящика либо пустота: по ней видно, сколько лежит голова.
---@return table|nil
function Module.first()
    local space = Module.get()
    local tuple = space ~= nil and space.index.primary:min() or nil

    return tuple ~= nil and row_of(tuple) or nil
end

--- Старший ключ ящика; у пустого — ноль.
---@return integer
function Module.top()
    local tuple = required().index.primary:max()

    return tuple ~= nil and tuple.key or 0
end

--- Пишет барьер поколения: его фиксация ждёт всех, кто встал в WAL раньше.
---@param at number Стенное время записи
function Module.barrier(at)
    required()

    box.space[Module.BARRIER]:replace({ Module.BARRIER_KEY, at })
end

--- Убирает отправленную строку.
---@param key integer
function Module.remove(key)
    required():delete(key)
end

--- Уводит строку в зарытые: удаление и запись причины — одной транзакцией,
--- иначе обрыв между ними потерял бы её вовсе.
---@param row table Строка ящика
---@param reason string Почему она не ушла
---@param at number Стенное время
function Module.bury(row, reason, at)
    local space = required()

    box.atomic(function()
        box.space[Module.DEAD]:replace({
            row.id,
            reason,
            at,
            { row.key, row.id, row.name, row.message, row.created },
        })
        space:delete(row.key)
    end)
end

--- Возвращает до `limit` зарытых в ящик — те же, что отдал бы `dead`, —
--- и отдаёт ключи возвращённых по порядку.
---
--- Перенос — одной транзакцией, как и зарытие: обрыв посередине не раздвоит
--- строку и не потеряет её. Выборка — в той же транзакции: под MVCC
--- строку, которую успел вернуть сосед, второй возврат не положит в ящик
--- ещё раз, а откатится конфликтом.
---
--- Ключ новый — от последовательности: старый лежит под границей
--- поколения, и вывоз счёл бы строку зафиксированной ещё до фиксации
--- возврата. Время записи — миг возврата: по нему считается возраст головы,
--- а он говорит, сколько строка ждёт вывоза, а не сколько ей лет.
---@param limit integer
---@param at number Стенное время возврата
---@return integer[] keys
function Module.revive(limit, at)
    local space = required()
    local dead = space_of(Module.DEAD)
    local keys = {}

    -- Зарытых ещё нет: заведение уступает между ящиком и ими, а у спейса
    -- и между ним и индексом. Возвращать в это окно нечего.
    if dead == nil or dead.index.primary == nil then
        return keys
    end

    box.atomic(function()
        for _, tuple in ipairs(dead:select({}, { limit = limit })) do
            local row = tuple.row

            keys[#keys + 1] = space:insert({ box.NULL, row[2], row[3], row[4], at })[1]
            dead:delete(tuple.id)
        end
    end)

    return keys
end

--- Зарытые: до `limit` строк от старых к новым.
---@param limit integer
---@return table[]
function Module.dead(limit)
    local space = space_of(Module.DEAD)
    local rows = {}

    for _, tuple in ipairs(space ~= nil and space:select({}, { limit = limit }) or {}) do
        rows[#rows + 1] = row_of(tuple)
    end

    return rows
end

--- Сколько строк ждёт вывоза и сколько зарыто; у незаведённого ящика —
--- пустота: ноль сказал бы, что ящик есть и он пуст.
---@return { pending: integer, dead: integer }|nil
function Module.depth()
    local space = Module.get()
    local dead = space_of(Module.DEAD)

    if space == nil or dead == nil then
        return nil
    end

    return { pending = space:len(), dead = dead:len() }
end

return Module
