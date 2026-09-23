--- Отметки фиксации: откуда вывоз знает, что строку ящика можно отправлять.
---
--- Без MVCC строку транзакции, ждущей записи в WAL или кворума синхронной
--- репликации, **видят все**: вывоз, прочитавший её, отправил бы брокеру
--- событие, которого после отката не было. `txn_isolation = 'read-confirmed'`
--- без MVCC ничего не меняет, а MVCC на узле бывает не включён.
---
--- Поэтому о фиксации говорят два независимых свидетельства:
---
---   * **отметка** — `box.on_commit` транзакции записи. Триггер приходит
---     после записи в WAL и после кворума и не приходит при откате. Внутри
---     него — только отметка и толчок вывозу: уступка в `on_commit` роняет
---     процесс, и потому даже будильник обязан не ждать;
---   * **граница поколения** — старший ключ, прочитанный перед записью
---     барьера. Фиксация барьера ждёт всех, кто встал в WAL раньше, и
---     откатывается вместе с ними: строки не старше границы, пережившие
---     барьер, зафиксированы. Так вывозится лежавшее до запуска — отметок
---     в памяти после перезапуска нет.
---
--- Один триггер на транзакцию, а не на строку: транзакция с сотней строк
--- ставила бы сотню триггеров, а отметить их можно разом. `on_rollback`
--- стоит рядом с `on_commit` затем, чтобы откат не оставлял за собой
--- список ключей: номера транзакций не повторяются, и забытый список
--- не вернул бы память никогда.

local external = require('tnt.external')

local Module = {}

--- Внешние средства: транзакция вызывающего и её исход.
local source = external.install(Module, {
    -- До `box.cfg` любой вопрос к box — исключение «Please call box.cfg{}
    -- first»: узла нет, значит нет и транзакции.
    in_txn = function()
        return type(box.cfg) ~= 'function' and box.is_in_txn()
    end,

    txn_id = function()
        return box.txn_id()
    end,

    on_commit = function(task)
        box.on_commit(task)
    end,

    on_rollback = function(task)
        box.on_rollback(task)
    end,
})

--- Ключи, о чьей фиксации ящик знает.
---@type table<integer, boolean>
local marks = {}

--- Ключи транзакций, которые ещё не кончились: номер транзакции — её ключи.
---@type table<integer, integer[]>
local pending = {}

--- Граница, пока поколение не открывали: ключи спейса начинаются
--- с единицы, и ни один не оказывается под ней.
local NO_GENERATION = 0

--- Граница поколения: всё, что не старше её, зафиксировано.
local boundary = NO_GENERATION

--- Кого будить, когда отметка пришла.
---@type fun()|nil
local waker = nil

--- Толкает вывоз: отметка пришла, и ждать своего срока такту незачем.
---
--- Зовётся из `box.on_commit`, поэтому не ждёт и не бросает: уступка там
--- роняет процесс, а бросок сорвал бы триггеры соседей.
local function notify()
    if waker ~= nil then
        pcall(waker)
    end
end

--- Называет, кого будить при отметке. Без слушателя отметка просто ложится.
---@param wake fun()|nil
function Module.on_mark(wake)
    waker = wake
end

--- Отмечает ключ строки: вне транзакции — сразу, в транзакции — после
--- её фиксации.
---
--- Вне транзакции вставка кончилась своей транзакцией, и она уже записана:
--- вызов вернулся, значит WAL (и кворум, если спейс синхронный) её приняли.
---@param key integer Ключ вставленной строки
function Module.mark(key)
    if not source().in_txn() then
        marks[key] = true

        return notify()
    end

    local txn = source().txn_id()
    local keys = pending[txn]

    if keys == nil then
        keys = {}
        pending[txn] = keys

        source().on_commit(function()
            -- Без уступки: уступка в `on_commit` роняет процесс.
            for _, committed in ipairs(keys) do
                marks[committed] = true
            end

            pending[txn] = nil
            notify()
        end)

        source().on_rollback(function()
            pending[txn] = nil
        end)
    end

    keys[#keys + 1] = key
end

--- Знает ли ящик о фиксации этой строки.
---@param key integer
---@return boolean
function Module.committed(key)
    return key <= boundary or marks[key] == true
end

--- Забывает отметку строки, которая уехала из ящика.
---@param key integer
function Module.forget(key)
    marks[key] = nil
end

--- Открывает поколение: всё не старше `top` считается зафиксированным.
---
--- Отметки под границей забываются здесь же: они больше ничего не говорят,
--- а список, который только растёт, однажды займёт всю память узла.
---@param top integer Старший ключ, прочитанный перед записью барьера
function Module.open(top)
    boundary = top

    for key in pairs(marks) do
        if key <= boundary then
            marks[key] = nil
        end
    end
end

--- Граница нынешнего поколения.
---@return integer
function Module.boundary()
    return boundary
end

--- Есть ли сейчас транзакция вызывающего.
---@return boolean
function Module.in_txn()
    return source().in_txn()
end

--- Что с отметками сейчас: граница, сколько отметок, сколько транзакций
--- ещё не кончились.
---@return { boundary: integer, marks: integer, pending: integer }
function Module.status()
    local marked, waiting = 0, 0

    for _ in pairs(marks) do
        marked = marked + 1
    end

    for _ in pairs(pending) do
        waiting = waiting + 1
    end

    return { boundary = boundary, marks = marked, pending = waiting }
end

--- Забывает отметки и границу. Нужен проверкам и перезапуску вывоза:
--- после смены ведущего поколение открывается заново.
---
--- Слушатель остаётся на месте: он не состояние ящика, а проводка — её
--- ставит фасад при загрузке и снимает `on_mark(nil)`.
function Module.reset()
    marks = {}
    pending = {}
    boundary = NO_GENERATION
end

return Module
