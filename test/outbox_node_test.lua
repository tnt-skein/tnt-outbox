--- Ящик на настоящем узле: запись в транзакции с данными, отметки
--- фиксации, ключ последовательности, вывоз и зарытые.
---
--- Двойника `box` здесь нет нарочно: ящик и есть работа с `box`, и
--- проверять двойником пришлось бы то, ради чего он написан, — видимость
--- незафиксированной строки, триггер фиксации, порядок ключей.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.outbox.node')

g.before_all(function()
    g.server = helper.start_node('outbox')
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.before_each(function()
    g.server:exec(function()
        local outbox = require('tnt.outbox')

        -- Ящик заводит вывоз, и проход без отправителей только заводит
        -- его: дальше проверка пишет в готовый.
        outbox.flush()

        for _, name in ipairs({ 'outbox', 'outbox_dead', 'outbox_barrier' }) do
            box.space[name]:truncate()
        end

        -- Счёт, отступ и поколение прошлой проверки забываются после
        -- уборки: проход по её строкам успел отложить вывоз.
        outbox.reset()
    end)
end)

--- Исполняет тело на узле.
---
--- Двойник отправителя берётся из `package.loaded`, а не `require`:
--- оснастка кладёт его на узел по имени, файла с таким путём нет, и
--- `require` в теле искал бы его по дереву узла.
---@param body function
---@return any
local function on_node(body)
    return g.server:exec(body)
end

g.test_a_write_outside_a_transaction_is_committed_and_shipped_at_once = function()
    local seen = on_node(function()
        local context = require('tnt.context')
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new(nil, {
            delay = true,
            key = true,
            priority = true,
            ttl = true,
        })

        outbox.configure({ senders = { ['order.paid'] = sender } })

        local written = context.run({ request_id = 'r-7' }, function()
            return outbox.write('order.paid', { order = 11 }, { key = 'o-11', delay = 2 })
        end)

        local lying = assert(box.space.outbox:select()[1], 'строки в ящике нет')
        local before = outbox.status()
        local report = outbox.flush()

        return {
            written = written,
            lying = { id = lying.id, name = lying.name, key = lying.key, message = lying.message },
            before = { pending = before.pending, written_count = before.counts.written },
            report = report,
            sent = sender.sent,
            left = box.space.outbox:count(),
            status = outbox.status(),
        }
    end)

    t.assert_equals(#seen.written, 26)
    t.assert_equals(seen.lying.id, seen.written)
    t.assert_equals(seen.lying.name, 'order.paid')
    t.assert_equals(seen.lying.message.body, { order = 11 })
    t.assert_equals(seen.lying.message.options.key, 'o-11')
    t.assert_equals(seen.lying.message.options.delay, 2)
    t.assert_equals(seen.before, { pending = 1, written_count = 1 })

    t.assert_equals(seen.report.sent, 1)
    t.assert_equals(#seen.sent, 1)
    t.assert_equals(seen.sent[1].name, 'order.paid')
    t.assert_equals(seen.sent[1].body, { order = 11 })
    t.assert_equals(seen.sent[1].opts.id, seen.written)
    t.assert_equals(seen.sent[1].opts.headers['x-request-id'], 'r-7')
    t.assert_equals(seen.sent[1].opts.key, 'o-11')
    t.assert_equals(seen.left, 0)
    t.assert_equals(seen.status.counts.sent, 1)
    t.assert_equals(seen.status.pending, 0)
end

g.test_a_rollback_takes_the_event_with_the_data_and_a_commit_lets_it_out = function()
    local seen = on_node(function()
        local context = require('tnt.context')
        local outbox = require('tnt.outbox')

        local orders = box.space.orders or box.schema.space.create('orders')

        orders:create_index('primary', { if_not_exists = true })
        orders:truncate()

        local sender = package.loaded['tnt.outbox.recorder'].new()

        outbox.configure({ senders = { ['order.paid'] = sender } })

        -- Откат: ни строки заказа, ни события.
        pcall(box.atomic, function()
            orders:insert({ 1 })
            outbox.write('order.paid', { order = 1 })

            box.rollback()
        end)

        local after_rollback = { orders = orders:count(), pending = box.space.outbox:count() }
        local shipped_after_rollback = outbox.flush().sent

        -- Фиксация: строка и событие уходят вместе, а опознаватель
        -- и заголовки у события — из мига записи.
        local written = context.run({ request_id = 'r-9' }, function()
            return box.atomic(function()
                orders:insert({ 2 })

                return outbox.write('order.paid', { order = 2 })
            end)
        end)

        local report = outbox.flush()

        return {
            after_rollback = after_rollback,
            shipped_after_rollback = shipped_after_rollback,
            written = written,
            report = report,
            sent = sender.sent,
            orders = orders:count(),
        }
    end)

    t.assert_equals(seen.after_rollback, { orders = 0, pending = 0 })
    t.assert_equals(seen.shipped_after_rollback, 0)
    t.assert_equals(seen.report.sent, 1)
    t.assert_equals(seen.orders, 1)
    t.assert_equals(#seen.sent, 1)
    t.assert_equals(seen.sent[1].opts.id, seen.written)
    t.assert_equals(seen.sent[1].opts.headers['x-request-id'], 'r-9')
end

g.test_the_key_of_the_row_does_not_start_over_when_the_outbox_empties = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        outbox.configure({ senders = { orders = package.loaded['tnt.outbox.recorder'].new() } })
        outbox.write('orders', 'первое')

        local first = assert(box.space.outbox:select()[1], 'строки в ящике нет').key

        outbox.flush()
        outbox.write('orders', 'второе')

        local second = assert(box.space.outbox:select()[1], 'второй строки нет').key

        return { first = first, second = second }
    end)

    -- Ключ ведёт последовательность спейса: опустевший ящик не начинает
    -- нумерацию заново, и новая строка не ложится под старую границу
    -- поколения.
    t.assert_equals(seen.second, seen.first + 1)
end

g.test_a_retriable_failure_keeps_the_rows_and_their_order = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local sender =
            package.loaded['tnt.outbox.recorder'].new({ { err = { message = 'брокер молчит' } } })

        outbox.configure({
            senders = { orders = sender },
            backoff = { base = 0, factor = 1, jitter = 0, max = 0 },
        })

        outbox.write('orders', 'первое')
        outbox.write('orders', 'второе')

        local refused = outbox.flush()
        local left = box.space.outbox:count()
        local report = outbox.flush()

        return { refused = refused, left = left, report = report, sent = sender.bodies }
    end)

    t.assert_equals(seen.refused.err, 'брокер молчит')
    t.assert_equals(seen.refused.sent, 0)
    t.assert_equals(seen.left, 2)
    t.assert_equals(seen.report.sent, 2)

    -- Порядок держится: голова ждёт брокера, а не пропускается.
    t.assert_equals(seen.sent, { 'первое', 'второе' })
end

g.test_an_unretriable_failure_goes_to_the_dead_with_the_whole_row = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new({
            { err = { message = 'брокер тело не принял', retriable = false } },
        })

        outbox.configure({ senders = { orders = sender } })

        local buried = outbox.write('orders', 'негодное')

        outbox.write('orders', 'годное')

        local report = outbox.flush()
        local dead = outbox.dead()
        local fields = {}

        for name in pairs(assert(dead[1], 'зарытой строки нет')) do
            table.insert(fields, tostring(name))
        end

        table.sort(fields)

        return {
            buried = buried,
            report = report,
            dead = dead,
            fields = fields,
            sent = sender.bodies,
            left = box.space.outbox:count(),
            status = outbox.status(),
        }
    end)

    t.assert_equals(seen.report, { sent = 1, dead = 1, more = false })
    t.assert_equals(seen.sent, { 'годное' })
    t.assert_equals(seen.left, 0)
    t.assert_equals(#seen.dead, 1)
    t.assert_equals(seen.dead[1].id, seen.buried)
    t.assert_equals(seen.dead[1].reason, 'брокер тело не принял')
    t.assert_equals(seen.dead[1].row[2], seen.buried)
    t.assert_equals(seen.dead[1].row[3], 'orders')
    t.assert_equals(seen.dead[1].row[4].body, 'негодное')
    t.assert_equals(seen.status.dead, 1)

    -- Строка зарытых отдаётся по именам полей, а не вперемешку с номерами.
    t.assert_equals(seen.fields, { 'buried', 'id', 'reason', 'row' })
end

g.test_a_kicked_row_goes_out_again_with_the_same_id_behind_the_rest = function()
    local seen = on_node(function()
        local context = require('tnt.context')
        local fiber = require('fiber')
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new({
            { err = { message = 'брокер тело не принял', retriable = false } },
        })

        outbox.configure({ senders = { orders = sender } })

        local buried = context.run({ request_id = 'r-5' }, function()
            return outbox.write('orders', 'починенное', { timeout = 3 })
        end)

        -- Проход зарывает строку и открывает поколение: граница теперь
        -- выше её старого ключа.
        outbox.flush()

        local lying = assert(box.space.outbox_dead:get(buried), 'строка не зарыта')

        -- Строка, записанная после зарытия, стоит в ящике впереди
        -- возвращённой; пауза разводит время записи и время возврата.
        outbox.write('orders', 'позже')
        fiber.sleep(0.01)

        local moved = outbox.kick(1)
        local back = {}

        for _, tuple in box.space.outbox:pairs() do
            back[#back + 1] = tuple:tomap({ names_only = true })
        end

        local report = outbox.flush()

        return {
            buried = buried,
            lying = { key = lying.row[1], created = lying.row[5], buried = lying.buried },
            moved = moved,
            back = back,
            left = box.space.outbox_dead:count(),
            report = report,
            sent = sender.sent,
            status = outbox.status(),
        }
    end)

    t.assert_equals(seen.moved, 1)
    t.assert_equals(seen.left, 0)

    -- Возвращённая встаёт в хвост с новым ключом: тот же опознаватель,
    -- назначение, тело и настройки, время — миг возврата.
    t.assert_equals(#seen.back, 2)
    t.assert_equals(seen.back[1].message.body, 'позже')

    local kicked = seen.back[2]

    t.assert_gt(kicked.key, seen.back[1].key)
    t.assert_gt(seen.back[1].key, seen.lying.key)
    t.assert_equals(kicked.id, seen.buried)
    t.assert_equals(kicked.name, 'orders')
    t.assert_equals(kicked.message.body, 'починенное')
    t.assert_equals(kicked.message.options.id, seen.buried)
    t.assert_equals(kicked.message.options.timeout, 3)
    t.assert_ge(kicked.created, seen.lying.buried)
    t.assert_ge(kicked.created - seen.lying.created, 0.01)

    -- Отметка фиксации поставлена: ближайший проход отправляет обе,
    -- не стоит на возвращённой и не выдаёт её за вставку мимо записи.
    t.assert_equals(seen.report, { sent = 2, dead = 0, more = false })
    t.assert_equals(#seen.sent, 3)
    t.assert_equals(seen.sent[2].body, 'позже')
    t.assert_equals(seen.sent[3].body, 'починенное')
    t.assert_equals(seen.sent[3].opts.id, seen.buried)
    t.assert_equals(seen.sent[3].opts.headers['x-request-id'], 'r-5')
    t.assert_equals(seen.sent[3].opts.timeout, 3)
    t.assert_equals(seen.status.counts.stale, 0)
    t.assert_equals(seen.status.counts.kicked, 1)
    t.assert_equals(seen.status.dead, 0)
end

g.test_a_kick_takes_the_dead_in_the_order_they_are_shown = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local refused = { err = { message = 'не принято', retriable = false } }
        local sender = package.loaded['tnt.outbox.recorder'].new({ refused, refused, refused })

        outbox.configure({ senders = { orders = sender } })

        -- Порядок записи нарочно не совпадает с порядком опознавателей:
        -- возврат берёт зарытые так же, как их показывает `dead`.
        outbox.write('orders', 'в', { id = 'c' })
        outbox.write('orders', 'а', { id = 'a' })
        outbox.write('orders', 'б', { id = 'b' })
        outbox.flush()

        local function ids(space)
            local listed = {}

            for _, tuple in space:pairs() do
                listed[#listed + 1] = tuple.id
            end

            return listed
        end

        local first = outbox.kick()
        local left = ids(box.space.outbox_dead)
        local rest = outbox.kick(5)
        local none = outbox.kick(5)
        local order = ids(box.space.outbox)

        outbox.flush()

        return {
            first = first,
            left = left,
            rest = rest,
            none = none,
            order = order,
            bodies = sender.bodies,
            status = outbox.status(),
        }
    end)

    -- Без числа возвращается одна — старшая из показанных.
    t.assert_equals(seen.first, 1)
    t.assert_equals(seen.left, { 'b', 'c' })
    t.assert_equals(seen.rest, 2)
    t.assert_equals(seen.none, 0)
    t.assert_equals(seen.order, { 'a', 'b', 'c' })
    t.assert_equals(seen.bodies, { 'а', 'б', 'в' })
    t.assert_equals(seen.status.counts.kicked, 3)
    t.assert_equals(seen.status.dead, 0)
end

g.test_a_kick_in_a_transaction_or_on_a_replica_leaves_the_dead_in_place = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new({
            { err = { message = 'не принято', retriable = false } },
        })

        outbox.configure({ senders = { orders = sender } })
        outbox.write('orders', 'зарытое')
        outbox.flush()

        local inside = select(
            2,
            pcall(box.atomic, function()
                return outbox.kick(1)
            end)
        )

        box.cfg({ read_only = true })

        local on_replica = select(2, pcall(outbox.kick, 1))

        box.cfg({ read_only = false })

        return {
            inside = tostring(inside),
            on_replica = tostring(on_replica),
            dead = box.space.outbox_dead:count(),
            pending = box.space.outbox:count(),
            kicked = outbox.status().counts.kicked,
        }
    end)

    t.assert_str_contains(
        seen.inside,
        'возврат зарытых идёт своей транзакцией, и в чужой его не зовут'
    )

    -- Возврат — запись, и на реплике он отказывает, как всякая запись,
    -- исключением box; перенос не начался, зарытое на месте.
    t.assert_str_contains(seen.on_replica, 'read-only')
    t.assert_equals(seen.dead, 1)
    t.assert_equals(seen.pending, 0)
    t.assert_equals(seen.kicked, 0)
end

g.test_a_kick_before_the_dead_are_built_returns_nothing = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        -- Окна заведения: ящик уже есть, а спейса зарытых ещё нет либо
        -- у него ещё нет индекса. Спейс вернёт первый проход следующей
        -- проверки.
        box.space.outbox_dead:drop()

        local absent = outbox.kick(1)

        box.schema.space.create('outbox_dead')

        local unindexed = outbox.kick(1)

        box.space.outbox_dead:drop()

        return { absent = absent, unindexed = unindexed, kicked = outbox.status().counts.kicked }
    end)

    t.assert_equals(seen, { absent = 0, unindexed = 0, kicked = 0 })
end

g.test_rows_that_lay_before_the_start_are_shipped_by_the_barrier = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new()

        outbox.configure({ senders = { orders = sender } })
        outbox.write('orders', 'лежало')

        -- Перезапуск: отметок в памяти нет, а строка в спейсе есть.
        outbox.reset()
        outbox.configure({ senders = { orders = sender } })

        local report = outbox.flush()

        return {
            report = report,
            sent = sender.bodies,
            boundary = outbox.status().boundary,
            barrier = box.space.outbox_barrier:select(),
        }
    end)

    t.assert_equals(seen.report.sent, 1)
    t.assert_equals(seen.sent, { 'лежало' })
    t.assert_not_equals(seen.boundary, 0)

    -- Барьер — одна строка на известном ключе: каждое поколение
    -- переписывает её, а не кладёт рядом новую.
    t.assert_equals(#seen.barrier, 1)
    t.assert_equals(seen.barrier[1][1], 1)
end

g.test_a_replica_ships_nothing_and_the_leader_opens_the_generation_anew = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new()

        outbox.configure({ senders = { orders = sender } })
        outbox.write('orders', 'до реплики')
        box.cfg({ read_only = true })

        local on_replica = outbox.flush()
        local refused = select(2, pcall(outbox.write, 'orders', 'на реплике'))

        box.cfg({ read_only = false })

        local report = outbox.flush()

        return {
            on_replica = on_replica,
            refused = tostring(refused),
            report = report,
            sent = sender.bodies,
        }
    end)

    t.assert_equals(seen.on_replica.skipped, 'узел только для чтения')
    t.assert_str_contains(seen.refused, 'read-only')
    t.assert_equals(seen.report.sent, 1)
    t.assert_equals(seen.sent, { 'до реплики' })
end

g.test_the_commit_wakes_the_relay_and_the_event_goes_without_waiting = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local outbox = require('tnt.outbox')

        local sender = package.loaded['tnt.outbox.recorder'].new()

        outbox.configure({ interval = 60, senders = { orders = sender } })
        outbox.start()

        -- Первый такт идёт сразу и открывает поколение; ждать его конца
        -- записи незачем — её разбудит отметка фиксации.
        box.atomic(function()
            outbox.write('orders', 'из транзакции')
        end)

        local clock = require('tnt.clock')
        local started = clock.monotonic()

        ---@type number
        local waited = 0

        while #sender.bodies == 0 and waited < 5 do
            fiber.sleep(0.01)

            waited = clock.monotonic() - started
        end

        local running = outbox.status().running

        outbox.stop()

        return {
            sent = sender.bodies,
            waited = waited,
            running = running,
            stopped = outbox.status().running,
        }
    end)

    t.assert_equals(seen.sent, { 'из транзакции' })

    -- Такт раз в минуту, а событие ушло за доли секунды: его вынесла
    -- отметка фиксации, а не срок такта.
    t.assert_lt(seen.waited, 5)
    t.assert_equals(seen.running, true)
    t.assert_equals(seen.stopped, false)
end

g.test_a_body_with_a_ring_is_refused_before_the_row = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        outbox.configure({ senders = { orders = package.loaded['tnt.outbox.recorder'].new() } })

        local ring = {}

        ring.self = ring

        return {
            refused = tostring(select(2, pcall(outbox.write, 'orders', ring))),
            pending = box.space.outbox:count(),
        }
    end)

    -- Кольцо ловит проверка тела при записи, до вставки строки: тот же
    -- отказ дал бы и отправитель, которому ящик отдаст тело на вывозе.
    t.assert_str_contains(
        seen.refused,
        'тело.self — кольцо: таблица лежит сама в себе'
    )
    t.assert_equals(seen.pending, 0)
end

g.test_an_outbox_caught_half_built_is_not_ready_for_writing = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')
        local space_of = require('tnt.outbox.space')

        outbox.configure({ senders = { orders = package.loaded['tnt.outbox.recorder'].new() } })
        box.space.outbox:drop()

        -- Окно заведения: спейс уже есть, а первичного индекса ещё нет —
        -- между ними DDL уступает, и сосед видит ящик недостроенным.
        box.schema.space.create('outbox')

        local ready = space_of.get() ~= nil
        local refused = tostring(select(2, pcall(outbox.write, 'orders', 'в окно')))
        local status = outbox.status()

        box.space.outbox:drop()

        return { ready = ready, refused = refused, pending = status.pending, dead = status.dead }
    end)

    t.assert_equals(seen.ready, false)

    -- Сводка в это окно не падает и не врёт: глубины у недостроенного
    -- ящика нет, и показывается ноль.
    t.assert_equals(seen.pending, 0)
    t.assert_equals(seen.dead, 0)
    t.assert_str_contains(
        seen.refused,
        'ящик ещё не заведён: позовите outbox.start() при подъёме узла'
    )
end

g.test_the_status_shows_the_depth_and_the_age_of_the_head = function()
    local seen = on_node(function()
        local outbox = require('tnt.outbox')

        outbox.configure({ senders = { orders = package.loaded['tnt.outbox.recorder'].new() } })

        -- Поколение открывается на пустом ящике: старшего ключа нет,
        -- и граница нулевая — под ней не окажется ни одна строка.
        outbox.flush()
        outbox.write('orders', 'лежит')

        local status = outbox.status()

        return {
            pending = status.pending,
            dead = status.dead,
            oldest = status.oldest_seconds,
            senders = status.senders,
            written = status.counts.written,
            boundary = status.boundary,
        }
    end)

    t.assert_equals(seen.pending, 1)
    t.assert_equals(seen.dead, 0)
    t.assert_equals(seen.boundary, 0)
    t.assert_equals(seen.senders, { 'orders' })
    t.assert_equals(seen.written, 1)
    t.assert_almost_equals(seen.oldest, 0, 5)
end

g.test_the_rows_of_the_outbox_follow_its_spaces = function()
    local seen = on_node(function()
        local metrics = require('metrics')
        local outbox = require('tnt.outbox')

        -- Числа рядов так, как их собирает выкладка: шкалы спрашивают
        -- спейсы на каждом сборе.
        local function rows()
            local found = {}

            for _, sample in ipairs(metrics.collect({ invoke_callbacks = true })) do
                local labels = sample.label_pairs

                found[('%s/%s'):format(sample.metric_name, labels.state or labels.destination or '')] = sample.value
            end

            return found
        end

        local sender = package.loaded['tnt.outbox.recorder'].new({
            { err = { message = 'брокер тело не принял', retriable = false } },
        })

        outbox.configure({ senders = { orders = sender } })

        local before = rows()

        outbox.write('orders', 'негодное')
        outbox.write('orders', 'годное')
        outbox.flush()
        outbox.write('orders', 'лежит')

        return { before = before, after = rows() }
    end)

    local before, after = seen.before, seen.after

    t.assert_equals(after['outbox_depth/pending'], 1)
    t.assert_equals(after['outbox_depth/dead'], 1)
    t.assert_almost_equals(after['outbox_oldest_seconds/'], 0, 5)
    t.assert_equals(after['outbox_sent_total/orders'] - (before['outbox_sent_total/orders'] or 0), 1)
    t.assert_equals(after['outbox_dead_total/orders'] - (before['outbox_dead_total/orders'] or 0), 1)
end
