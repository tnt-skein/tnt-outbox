--- Отметки фиксации за внешней зависимостью: один триггер на транзакцию, граница
--- поколения, толчок вывозу.
---
--- Настоящие `box.on_commit` и `box.on_rollback` проверяются на узле
--- (`outbox_node_test.lua`): здесь — то, что решает сам модуль, и ветки,
--- которых на узле не добиться, — сорвавшийся будильник.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local commit = helper.commit

local g = t.group('tnt.outbox.commit')

--- Внешняя зависимость транзакции: триггеры запоминаются, а не ставятся.
---@param in_txn boolean
---@param txn integer|nil
---@return table
local function transaction(in_txn, txn)
    local triggers = { commits = {}, rollbacks = {} }

    commit._set_source({
        in_txn = function()
            return in_txn
        end,
        txn_id = function()
            return txn or 1
        end,
        on_commit = function(task)
            table.insert(triggers.commits, task)
        end,
        on_rollback = function(task)
            table.insert(triggers.rollbacks, task)
        end,
    })

    return triggers
end

g.before_each(function()
    commit.reset()
    commit.on_mark(nil)
end)

g.after_each(function()
    commit._set_source(nil)
    commit.on_mark(nil)
end)

g.test_a_row_written_outside_a_transaction_is_committed_at_once = function()
    local woken = 0

    transaction(false)
    commit.on_mark(function()
        woken = woken + 1
    end)
    commit.mark(7)

    t.assert_equals(commit.committed(7), true)
    t.assert_equals(commit.committed(8), false)
    t.assert_equals(woken, 1)
end

g.test_a_transaction_marks_its_keys_only_after_the_commit = function()
    local triggers = transaction(true, 42)
    local woken = 0

    commit.on_mark(function()
        woken = woken + 1
    end)
    commit.mark(1)
    commit.mark(2)

    -- Триггер один на транзакцию, а не на строку.
    t.assert_equals(#triggers.commits, 1)
    t.assert_equals(#triggers.rollbacks, 1)
    t.assert_equals(commit.committed(1), false)
    t.assert_equals(commit.status(), { boundary = 0, marks = 0, pending = 1 })
    t.assert_equals(woken, 0)

    triggers.commits[1]()

    t.assert_equals(commit.committed(1), true)
    t.assert_equals(commit.committed(2), true)
    t.assert_equals(commit.status(), { boundary = 0, marks = 2, pending = 0 })
    t.assert_equals(woken, 1)
end

g.test_a_rollback_forgets_the_keys_of_the_transaction = function()
    local triggers = transaction(true, 42)

    commit.mark(1)
    triggers.rollbacks[1]()

    t.assert_equals(commit.committed(1), false)
    t.assert_equals(commit.status(), { boundary = 0, marks = 0, pending = 0 })

    -- Следующая транзакция того же номера ставит свои триггеры заново:
    -- список прежней не остался за ней.
    commit.mark(2)

    t.assert_equals(#triggers.commits, 2)
end

g.test_a_waker_that_broke_does_not_break_the_commit_trigger = function()
    transaction(false)
    commit.on_mark(function()
        error('будильник сорвался')
    end)

    commit.mark(3)

    t.assert_equals(commit.committed(3), true)
end

g.test_the_generation_boundary_covers_what_lay_before_it = function()
    transaction(false)
    commit.mark(5)
    commit.mark(9)
    commit.open(7)

    t.assert_equals(commit.boundary(), 7)
    t.assert_equals(commit.committed(7), true)
    t.assert_equals(commit.committed(8), false)
    t.assert_equals(commit.committed(9), true)

    -- Отметки под границей забыты: они больше ничего не говорят.
    t.assert_equals(commit.status(), { boundary = 7, marks = 1, pending = 0 })
end

g.test_a_mark_right_on_the_boundary_is_forgotten_with_the_ones_below = function()
    transaction(false)
    commit.mark(7)
    commit.open(7)

    -- Граница входит в поколение: строка с ключом ровно на ней уже
    -- зафиксирована, и отметке рядом с ней делать нечего.
    t.assert_equals(commit.committed(7), true)
    t.assert_equals(commit.status(), { boundary = 7, marks = 0, pending = 0 })
end

g.test_a_row_that_left_the_outbox_is_forgotten = function()
    transaction(false)
    commit.mark(4)
    commit.forget(4)

    t.assert_equals(commit.committed(4), false)
    t.assert_equals(commit.status().marks, 0)
end

g.test_the_transaction_of_the_caller_is_seen_through_the_externals = function()
    transaction(true, 3)

    t.assert_equals(commit.in_txn(), true)

    transaction(false)

    t.assert_equals(commit.in_txn(), false)
end

g.test_reset_forgets_the_marks_and_the_boundary = function()
    transaction(false)
    commit.mark(1)
    commit.open(1)
    commit.reset()

    t.assert_equals(commit.status(), { boundary = 0, marks = 0, pending = 0 })
end
