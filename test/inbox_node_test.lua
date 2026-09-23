--- Отметки получателя на настоящем узле: отметка в транзакции обработчика,
--- её откат, уборка по сроку и молчание уборки на реплике.
---
--- Двойника `box` здесь нет нарочно: отметка и есть вставка в транзакцию
--- вызывающего, и проверять двойником пришлось бы то, ради чего модуль
--- написан, — фиксацию и откат вместе с записью обработчика.
---
--- Стенные часы подменяются: срок хранения меряется ими, и отметка
--- «старше срока» ставится на заданный миг, а не ждётся неделю.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.outbox.inbox.node')

g.before_all(function()
    g.server = helper.start_node('inbox')
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.before_each(function()
    g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')

        -- Прошлая проверка могла упасть, не вернув узел в запись.
        box.cfg({ read_only = false })

        inbox.reset()
        inbox._set_source(nil)

        -- Проход уборки заводит спейс, если его унесла прошлая проверка.
        inbox.sweep()
        box.space.outbox_inbox:truncate()
        inbox.reset()

        local receipts = box.space.receipts or box.schema.space.create('receipts')

        receipts:create_index('primary', { if_not_exists = true, parts = { { 1, 'string' } } })
        receipts:truncate()
    end)
end)

g.after_each(function()
    g.server:exec(function()
        require('tnt.outbox.inbox').stop()
    end)
end)

g.test_two_deliveries_of_one_id_leave_one_record_of_the_handler = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local receipts = box.space.receipts
        local now = { value = 1000 }

        inbox._set_source({
            realtime = function()
                return now.value
            end,
        })

        --- Обработчик чеков: запись и отметка — одной транзакцией.
        local function handle(message)
            return box.atomic(function()
                if not inbox.claim('receipts', message) then
                    return false
                end

                receipts:insert({ message.id, message.body.order })

                return true
            end)
        end

        local message = { id = '01JA-7', body = { order = 7 } }
        local first = handle(message)
        local second = handle(message)

        -- Ответ на повтор — именно `false`, а не пустота: условие
        -- `== false` у обработчика приняло бы пустоту за первую доставку.
        local repeated = box.atomic(inbox.claim, 'receipts', message)

        -- Другой получатель того же события: у него свои отметки.
        local other = box.atomic(inbox.claim, 'mail', message)

        now.value = 1030

        return {
            first = first,
            second = second,
            repeated = repeated,
            other = other,
            receipts = receipts:select(),
            marks = box.space.outbox_inbox:select(),
            status = inbox.status(),
        }
    end)

    t.assert_equals(seen.first, true)
    t.assert_equals(seen.second, false)
    t.assert_equals(seen.repeated, false)
    t.assert_equals(seen.other, true)
    t.assert_equals(seen.receipts, { { '01JA-7', 7 } })
    t.assert_equals(seen.marks, { { 'mail', '01JA-7', 1000 }, { 'receipts', '01JA-7', 1000 } })
    t.assert_equals(seen.status.marks, 2)
    t.assert_equals(seen.status.oldest_seconds, 30)
    t.assert_equals(seen.status.counts, { claimed = 2, repeated = 2, removed = 0 })
end

g.test_a_rollback_of_the_handler_takes_the_mark_with_it = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local receipts = box.space.receipts
        local message = { id = '01JA-8', body = { order = 8 } }

        --- Обработчик, упавший после отметки и записи.
        local function broken()
            inbox.claim('receipts', message)
            receipts:insert({ message.id, message.body.order })
            error('обработчик упал', 0)
        end

        -- Откат снимает и запись, и отметку.
        local failed = select(2, pcall(box.atomic, broken))

        local after_rollback = { marks = box.space.outbox_inbox:count(), receipts = receipts:count() }

        -- Повтор после отката обрабатывается заново, как и должен.
        local retried = box.atomic(function()
            local first = inbox.claim('receipts', message)

            receipts:insert({ message.id, message.body.order })

            return first
        end)

        return {
            failed = failed,
            after_rollback = after_rollback,
            retried = retried,
            marks = box.space.outbox_inbox:count(),
            receipts = receipts:count(),
        }
    end)

    t.assert_equals(seen.failed, 'обработчик упал')
    t.assert_equals(seen.after_rollback, { marks = 0, receipts = 0 })
    t.assert_equals(seen.retried, true)
    t.assert_equals(seen.marks, 1)
    t.assert_equals(seen.receipts, 1)
end

g.test_a_claim_outside_a_transaction_leaves_no_mark = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local message = { id = '01JA-9' }
        local refused = select(2, pcall(inbox.claim, 'receipts', message))

        return { refused = tostring(refused), marks = box.space.outbox_inbox:count() }
    end)

    t.assert_str_contains(
        seen.refused,
        'отметку ставят в транзакции обработчика: без его записи она повтор не отсекает'
    )
    t.assert_equals(seen.marks, 0)
end

g.test_the_sweep_removes_marks_older_than_the_retention_and_keeps_the_rest = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local now = { value = 1000 }

        inbox._set_source({
            realtime = function()
                return now.value
            end,
        })

        for _, at in ipairs({ { 'old', 1000 }, { 'edge', 1001 }, { 'young', 1100 } }) do
            now.value = at[2]
            box.atomic(inbox.claim, 'receipts', { id = at[1] })
        end

        inbox.configure({ retention = 100 })

        -- Горизонт — 1001: отметка ровно на нём не старше срока и остаётся.
        now.value = 1101

        local report = inbox.sweep()
        local left = {}

        for _, tuple in box.space.outbox_inbox:pairs() do
            table.insert(left, tuple.id)
        end

        table.sort(left)

        return { report = report, left = left, status = inbox.status() }
    end)

    t.assert_equals(seen.report, { removed = 1, more = false })
    t.assert_equals(seen.left, { 'edge', 'young' })
    t.assert_equals(seen.status.marks, 2)
    t.assert_equals(seen.status.counts.removed, 1)
end

g.test_a_full_batch_is_one_transaction_and_asks_for_the_next = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local now = { value = 1000 }

        inbox._set_source({
            realtime = function()
                return now.value
            end,
        })

        -- Три отметки в один миг: индекс времени неуникален.
        for _, id in ipairs({ 'a', 'b', 'c' }) do
            box.atomic(inbox.claim, 'receipts', { id = id })
        end

        inbox.configure({ retention = 100, batch = 2 })
        now.value = 2000

        local space = box.space.outbox_inbox
        local transactions = {}

        --- Номер транзакции каждого стирания.
        local function witness()
            transactions[box.txn_id()] = true
        end

        space:on_replace(witness)

        local first = inbox.sweep()

        space:on_replace(nil, witness)

        local distinct = 0

        for _ in pairs(transactions) do
            distinct = distinct + 1
        end

        return {
            first = first,
            transactions = distinct,
            second = inbox.sweep(),
            third = inbox.sweep(),
            status = inbox.status(),
        }
    end)

    t.assert_equals(seen.first, { removed = 2, more = true })

    -- Кусок — одна транзакция: транзакция на каждое стирание писала бы
    -- в WAL и уступала бы столько раз, сколько отметок.
    t.assert_equals(seen.transactions, 1)
    t.assert_equals(seen.second, { removed = 1, more = false })
    t.assert_equals(seen.third, { removed = 0, more = false })
    t.assert_equals(seen.status.marks, 0)
    t.assert_equals(seen.status.oldest_seconds, nil)
    t.assert_equals(seen.status.counts.removed, 3)
end

g.test_a_replica_removes_nothing_and_the_leader_does = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')
        local now = { value = 1000 }

        inbox._set_source({
            realtime = function()
                return now.value
            end,
        })

        box.atomic(inbox.claim, 'receipts', { id = 'old' })
        inbox.configure({ retention = 100 })
        now.value = 5000

        box.cfg({ read_only = true })

        local on_replica = inbox.sweep()
        local kept = box.space.outbox_inbox:count()

        box.cfg({ read_only = false })

        local on_leader = inbox.sweep()

        return {
            on_replica = on_replica,
            kept = kept,
            on_leader = on_leader,
            left = box.space.outbox_inbox:count(),
        }
    end)

    t.assert_equals(
        seen.on_replica,
        { removed = 0, more = false, skipped = 'узел только для чтения' }
    )
    t.assert_equals(seen.kept, 1)
    t.assert_equals(seen.on_leader, { removed = 1, more = false })
    t.assert_equals(seen.left, 0)
end

g.test_the_start_builds_the_space_on_a_node_that_writes = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')

        box.space.outbox_inbox:drop()

        -- До заведения сводка не падает: отметок нет, и старшей тоже.
        local before = inbox.status()

        inbox.start()

        local space = box.space.outbox_inbox
        local fields = {}

        for _, field in ipairs(space:format()) do
            table.insert(fields, { field.name, field.type })
        end

        local running = inbox.status().running

        inbox.stop()

        return {
            before = { marks = before.marks, oldest = before.oldest_seconds },
            fields = fields,
            engine = space.engine,
            primary = { space.index.primary.parts[1].fieldno, space.index.primary.parts[2].fieldno },
            marked = { space.index.marked.parts[1].fieldno, space.index.marked.unique },
            running = running,
            stopped = inbox.status().running,
        }
    end)

    t.assert_equals(seen.before, { marks = 0 })
    t.assert_equals(seen.fields, { { 'receiver', 'string' }, { 'id', 'string' }, { 'marked', 'number' } })
    t.assert_equals(seen.engine, 'memtx')
    t.assert_equals(seen.primary, { 1, 2 })
    t.assert_equals(seen.marked, { 3, false })
    t.assert_equals(seen.running, true)
    t.assert_equals(seen.stopped, false)
end

g.test_a_tick_that_removed_a_full_batch_takes_the_next_at_once = function()
    local seen = g.server:exec(function()
        local fiber = require('fiber')
        local inbox = require('tnt.outbox.inbox')
        local now = { value = 1000 }

        inbox._set_source({
            realtime = function()
                return now.value
            end,
        })

        for _, id in ipairs({ 'a', 'b', 'c', 'd' }) do
            box.atomic(inbox.claim, 'receipts', { id = id })
        end

        -- Такт раз в минуту и кусок в одну отметку: без побудки после
        -- полного куска четыре отметки уходили бы четыре минуты.
        inbox.configure({ retention = 100, batch = 1, interval = 60 })
        now.value = 2000
        inbox.start()

        local deadline = fiber.clock() + 5

        while box.space.outbox_inbox:count() > 0 and fiber.clock() < deadline do
            fiber.sleep(0.01)
        end

        return { left = box.space.outbox_inbox:count(), removed = inbox.status().counts.removed }
    end)

    t.assert_equals(seen.left, 0)
    t.assert_equals(seen.removed, 4)
end

g.test_a_half_built_space_is_not_ready_for_marks = function()
    local seen = g.server:exec(function()
        local inbox = require('tnt.outbox.inbox')

        box.space.outbox_inbox:drop()

        -- Окно заведения: спейс и первичный индекс уже есть, индекса
        -- времени ещё нет — между ними DDL уступает.
        local space = box.schema.space.create('outbox_inbox')

        space:create_index('primary', { parts = { { 1, 'string' }, { 2, 'string' } } })

        local refused = select(2, pcall(box.atomic, inbox.claim, 'receipts', { id = 'm-1' }))
        local status = inbox.status()

        space:drop()

        return { refused = tostring(refused), marks = status.marks, oldest = status.oldest_seconds }
    end)

    t.assert_str_contains(
        seen.refused,
        'отметки получателя ещё не заведены: позовите inbox.start() при подъёме узла'
    )
    t.assert_equals(seen.marks, 0)
    t.assert_equals(seen.oldest, nil)
end
