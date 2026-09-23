--- Неподтверждённая строка синхронной транзакции: видна всем, а вывозу —
--- нет.
---
--- Без MVCC строку транзакции, ждущей кворума, видит простое чтение
--- соседа. Вывоз, судящий по видимости, отправил бы брокеру событие,
--- которого после отката не было, — и взять его назад было бы нечем.
--- Поэтому узел здесь свой: кворум и владение очередью синхронизации
--- меняют поведение всякой записи, и соседним проверкам этого не надо.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.outbox.sync')

g.before_all(function()
    g.server = helper.start_node('sync')
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.test_a_row_waiting_for_the_quorum_is_visible_but_not_shipped = function()
    local seen = g.server:exec(function()
        local fiber = require('fiber')
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new()

        outbox.configure({ senders = { ['order.paid'] = sender } })

        -- Поколение открывается до опыта: барьер — тоже запись, и за
        -- ждущей кворума транзакцией он встал бы в очередь сам.
        outbox.flush()

        -- Кворум из двух на одиночном узле: транзакция не соберёт его
        -- никогда и откатится по сроку.
        box.ctl.promote()
        box.cfg({ replication_synchro_quorum = 2, replication_synchro_timeout = 0.3 })

        local orders = box.schema.space.create('sync_orders', { is_sync = true })

        orders:create_index('primary', {})

        local outcome = nil

        fiber.create(function()
            outcome = tostring(select(
                2,
                pcall(box.atomic, function()
                    orders:insert({ 1 })
                    outbox.write('order.paid', { order = 'sync' })
                end)
            ))
        end)

        fiber.sleep(0.05)

        -- Строка ждущей транзакции видна простым чтением: без MVCC её
        -- не спрятать.
        local visible = box.space.outbox:count()
        local during = outbox.flush()

        fiber.sleep(0.5)

        local after = { pending = box.space.outbox:count(), orders = orders:count() }
        local report = outbox.flush()

        box.cfg({ replication_synchro_quorum = 1 })

        return {
            visible = visible,
            during = during,
            outcome = outcome,
            after = after,
            report = report,
            sent = sender.bodies,
        }
    end)

    t.assert_equals(seen.visible, 1)
    t.assert_equals(seen.during.sent, 0)
    t.assert_equals(seen.during.stalled, 1)
    t.assert_str_contains(seen.outcome, 'imed out')

    -- Транзакция откатилась, и строки ящика не стало: отправлять нечего
    -- и отправлено ничего.
    t.assert_equals(seen.after, { pending = 0, orders = 0 })
    t.assert_equals(seen.report.sent, 0)
    t.assert_equals(seen.sent, {})
end
