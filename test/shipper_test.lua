--- Проход вывоза на двойнике спейсов: порядок, отметки фиксации, итоги
--- отправителя, отступ, отчёт и ряды ящика.
---
--- Настоящие спейсы проверяются на узле (`outbox_node_test.lua`); здесь
--- решения самого вывоза — что он делает с каждым итогом и когда кончает
--- проход.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local commit = helper.commit
local series = helper.series
local shipper = helper.shipper

local g = t.group('tnt.outbox.shipper')

--- Отступ без разброса: проверке нужна не случайность, а степень.
local BACKOFF = { base = 1, factor = 2, jitter = 0, max = 60 }

--- Стенд вывоза: двойник спейсов, часы и режим узла под рукой проверки.
---@param rows table[]|nil
---@return table
local function stand(rows)
    local it = { spaces = helper.spaces(rows), now = 100, realtime = 5000, read_only = false }

    shipper._set_source({
        space = it.spaces,
        now = function()
            return it.now
        end,
        realtime = function()
            return it.realtime
        end,
        read_only = function()
            return it.read_only
        end,
    })

    return it
end

--- Настраивает вывоз отправителями по именам.
---@param senders table<string, table>
---@param batch integer|nil
local function configured(senders, batch)
    shipper.configure({ senders = senders, batch = batch or 10, backoff = BACKOFF })
end

g.before_each(function()
    shipper.reset()
    commit.reset()
    commit._set_source({
        in_txn = function()
            return false
        end,
    })
    helper.journal.forget()
end)

g.after_each(function()
    shipper._set_source(nil)
    commit._set_source(nil)
end)

g.test_the_defaults_of_the_relay_are_the_written_ones = function()
    -- Числа здесь свои, а не из модуля: умолчание — обещание тому, кто
    -- ящик не настраивал, и сдвинься оно, проверка обязана упасть,
    -- а не сдвинуться вместе с ним.
    t.assert_equals(shipper.DEFAULT_BATCH, 100)
    t.assert_equals(shipper.STALE, 5)
    t.assert_equals(shipper.DEFAULT_BACKOFF, { base = 1, factor = 2, jitter = 0.5, max = 60 })
end

g.test_a_pass_sends_the_head_in_order_and_removes_what_went = function()
    local it = stand({ helper.row(1, 'order.paid', { order = 1 }), helper.row(2, 'order.paid', 'второе') })
    local sender = helper.sender()

    configured({ ['order.paid'] = sender })

    local report = shipper.flush()

    t.assert_equals(report.sent, 2)
    t.assert_equals(report.dead, 0)
    t.assert_equals(report.more, false)
    t.assert_equals(#it.spaces.rows, 0)

    -- Барьер поколения пишется один раз, на первом проходе.
    t.assert_equals(it.spaces.barriers, 1)
    t.assert_equals(commit.boundary(), 2)
    t.assert_equals(shipper.status().generation, it.realtime)

    t.assert_equals(sender.sent[1].name, 'order.paid')
    t.assert_equals(sender.sent[1].body, { order = 1 })
    t.assert_equals(sender.sent[2].body, 'второе')
    t.assert_equals(shipper.status().counts.sent, 2)
end

g.test_the_sender_gets_the_identifier_and_the_headers_of_the_writer = function()
    local row = helper.row(1, 'order.paid', { order = 1 }, {
        id = 'ready',
        headers = { ['x-request-id'] = 'r-9' },
        key = 'o-1',
    })
    local sender = helper.sender()

    stand({ row })
    configured({ ['order.paid'] = sender })
    shipper.flush()

    t.assert_equals(sender.sent[1].opts.id, 'ready')
    t.assert_equals(sender.sent[1].opts.headers, { ['x-request-id'] = 'r-9' })
    t.assert_equals(sender.sent[1].opts.key, 'o-1')
end

g.test_a_full_batch_says_there_is_more_to_ship = function()
    local it = stand({ helper.row(1, 'a', 1), helper.row(2, 'a', 2), helper.row(3, 'a', 3) })

    configured({ a = helper.sender() }, 2)

    local report = shipper.flush()

    t.assert_equals(report.sent, 2)
    t.assert_equals(report.more, true)
    t.assert_equals(#it.spaces.rows, 1)
end

g.test_two_passes_do_not_go_at_once = function()
    local it = stand({ helper.row(1, 'a', 1) })
    local head = it.spaces.head
    local inner = nil

    -- Такт и позванный руками проход приходятся друг на друга; второй
    -- обязан уйти ни с чем, иначе строку, которую первый уже отправил
    -- и ещё не удалил, он отправил бы второй раз.
    it.spaces.head = function(limit)
        inner = inner or shipper.flush()

        return head(limit)
    end

    configured({ a = helper.sender() })

    local report = shipper.flush()

    t.assert_equals(inner, { sent = 0, dead = 0, more = false, skipped = 'проход уже идёт' })
    t.assert_equals(report.sent, 1)
end

g.test_a_pass_that_broke_lets_the_next_one_go = function()
    local it = stand({ helper.row(1, 'a', 1) })

    it.spaces.head = function()
        error('спейс пропал', 0)
    end

    configured({ a = helper.sender() })

    -- Брошенное отдаётся как есть, без места внутри вывоза: сорвался
    -- спейс, и показывать на строку прохода вызывающему незачем.
    t.assert_error_msg_equals('спейс пропал', shipper.flush)

    -- Сорвавшийся проход не оставляет вывоз занятым навсегда.
    it.spaces.head = helper.spaces(it.spaces.rows).head

    t.assert_equals(shipper.flush().sent, 1)
end

g.test_a_replica_ships_nothing_and_opens_the_generation_anew = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender() })
    it.read_only = true

    t.assert_equals(
        shipper.flush(),
        { sent = 0, dead = 0, more = false, skipped = 'узел только для чтения' }
    )
    t.assert_equals(it.spaces.barriers, 0)
    t.assert_equals(#it.spaces.rows, 1)

    it.read_only = false
    shipper.flush()

    t.assert_equals(it.spaces.barriers, 1)
    t.assert_equals(#it.spaces.rows, 0)
end

g.test_a_row_whose_commit_is_unknown_stops_the_pass = function()
    local it = stand({ helper.row(1, 'a', 1) })
    local sender = helper.sender()

    configured({ a = sender })
    shipper.flush()

    -- Строка легла после того, как поколение открылось, и отметки у неё нет.
    table.insert(it.spaces.rows, helper.row(2, 'a', 2))
    table.insert(it.spaces.rows, helper.row(3, 'a', 3))

    local report = shipper.flush()

    t.assert_equals(report, { sent = 0, dead = 0, more = false, stalled = 2 })
    t.assert_equals(#sender.sent, 1)

    -- Отметка пришла — строка уходит, и соседи за ней.
    commit.mark(2)
    commit.mark(3)

    t.assert_equals(shipper.flush().sent, 2)
end

g.test_a_row_that_lay_past_the_stale_window_reopens_the_generation = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender() })
    shipper.flush()

    table.insert(it.spaces.rows, helper.row(2, 'a', 2))

    -- Первый проход только запоминает, с какого мига стоит строка.
    t.assert_equals(shipper.flush().stalled, 2)

    it.now = it.now + shipper.STALE - 0.01

    t.assert_equals(shipper.flush().stalled, 2)
    t.assert_equals(shipper.status().counts.stale, 0)

    -- Ровно `STALE` — уже довольно: граница входит в срок ожидания.
    it.now = it.now + 0.01

    t.assert_equals(shipper.flush().sent, 1)
    t.assert_equals(shipper.status().counts.stale, 1)
    t.assert_equals(it.spaces.barriers, 2)
    t.assert_equals(
        helper.journal.logged(
            'строка ящика легла мимо outbox.write: вывоз по границе'
        ),
        true
    )
end

g.test_a_retriable_failure_leaves_the_row_and_holds_the_pass_off = function()
    local it = stand({ helper.row(1, 'a', 1), helper.row(2, 'a', 2) })
    local sender = helper.sender({ { err = { message = 'брокер молчит' } } })

    configured({ a = sender })

    local report = shipper.flush()

    t.assert_equals(report.sent, 0)
    t.assert_equals(report.err, 'брокер молчит')
    t.assert_equals(#it.spaces.rows, 2)
    t.assert_equals(shipper.status().counts.failures, 1)
    t.assert_equals(shipper.status().holding, true)
    t.assert_equals(
        helper.journal.logged('вывоз ящика отложен: отказ отправителя'),
        true
    )

    -- До конца отступа вывоз не ходит к отправителю вовсе.
    t.assert_equals(shipper.flush().skipped, 'отступ после отказа')
    t.assert_equals(#sender.sent, 1)

    it.now = it.now + 1

    t.assert_equals(shipper.flush().sent, 2)
    t.assert_equals(shipper.status().holding, false)
end

g.test_the_hold_off_grows_with_the_failures_in_a_row = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender({ { err = 'раз' }, { err = 'два' } }) })

    shipper.flush()
    it.now = it.now + 1
    shipper.flush()
    it.now = it.now + 1

    -- Вторая пауза вдвое длиннее первой: до её конца проход не идёт.
    t.assert_equals(shipper.flush().skipped, 'отступ после отказа')
    t.assert_equals(shipper.status().counts.failures, 2)
end

g.test_a_good_pass_forgets_the_failures_before_it = function()
    local it = stand({ helper.row(1, 'a', 1) })
    -- Отказ, удачный проход, снова отказ: отступ после последнего обязан
    -- быть начальным, а не удвоенным, — иначе счёт отказов копится вечно
    -- и вывоз засыпает надолго после первой же беды.
    local answers = { { err = 'раз' }, { id = 'ушло' }, { err = 'два' } }

    configured({ a = helper.sender(answers) })

    t.assert_equals(shipper.flush().err, 'раз')

    it.now = it.now + 1

    t.assert_equals(shipper.flush().sent, 1)

    table.insert(it.spaces.rows, helper.row(2, 'a', 2))
    commit.mark(2)

    t.assert_equals(shipper.flush().err, 'два')

    -- Отступ после него — ровно начальный: половины мало, целой довольно.
    it.now = it.now + 0.5

    t.assert_equals(shipper.flush().skipped, 'отступ после отказа')

    it.now = it.now + 0.5

    t.assert_equals(shipper.flush().sent, 1)
    t.assert_equals(shipper.status().counts.sent, 2)
end

g.test_a_sender_that_broke_is_a_retriable_failure = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender({ { raise = 'соединение оборвалось' } }) })

    t.assert_equals(shipper.flush().err, 'соединение оборвалось')
    t.assert_equals(#it.spaces.rows, 1)
end

g.test_a_sender_that_explained_nothing_is_a_retriable_failure = function()
    stand({ helper.row(1, 'a', 1) })
    configured({ a = helper.sender({ {} }) })

    t.assert_equals(shipper.flush().err, 'отправитель отказал и причины не назвал')
end

g.test_a_name_without_a_sender_waits_for_the_settings = function()
    local it = stand({ helper.row(1, 'order.paid', 1) })

    configured({ other = helper.sender() })

    t.assert_equals(shipper.flush().err, 'отправителя «order.paid» ящику не назначили')
    t.assert_equals(#it.spaces.rows, 1)
end

g.test_an_unretriable_failure_goes_to_the_dead_and_the_pass_goes_on = function()
    local it = stand({ helper.row(1, 'a', 'негодное'), helper.row(2, 'a', 'годное') })
    local sender =
        helper.sender({ { err = { message = 'брокер тело не принял', retriable = false } } })

    configured({ a = sender })

    local report = shipper.flush()

    t.assert_equals(report, { sent = 1, dead = 1, more = false })
    t.assert_equals(#it.spaces.rows, 0)
    t.assert_equals(it.spaces.dead[1].reason, 'брокер тело не принял')
    t.assert_equals(it.spaces.dead[1].id, 'id-1')
    t.assert_equals(it.spaces.dead[1].buried, it.realtime)
    local buried = helper.journal.find('строка ящика зарыта')

    t.assert_equals(buried.level, 'error')
    t.assert_equals(buried.record.fields.err, 'брокер тело не принял')
    t.assert_equals(shipper.status().counts.dead, 1)
end

g.test_the_status_shows_the_depth_and_the_age_of_the_head = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender() }, 5)

    local status = shipper.status()

    t.assert_equals(status.batch, 5)
    t.assert_equals(status.pending, 1)
    t.assert_equals(status.dead, 0)
    t.assert_equals(status.oldest_seconds, it.realtime - 1001)
    t.assert_equals(status.boundary, 0)
    t.assert_equals(status.holding, false)
    t.assert_equals(status.counts, { sent = 0, dead = 0, failures = 0, stale = 0 })
end

g.test_the_status_of_an_empty_outbox_has_no_head = function()
    stand({})

    t.assert_equals(shipper.status().oldest_seconds, nil)
    t.assert_equals(shipper.status().pending, 0)
end

g.test_the_sender_of_a_name_is_told_by_the_settings = function()
    local sender = helper.sender()

    configured({ ['order.paid'] = sender })

    t.assert_equals(shipper.sender('order.paid'), sender)
    t.assert_equals(shipper.sender('order.shipped'), nil)
end

g.test_reset_forgets_the_count_the_hold_off_and_the_generation = function()
    local it = stand({ helper.row(1, 'a', 1) })

    configured({ a = helper.sender({ { err = 'молчит' } }) })
    shipper.flush()
    shipper.reset()

    t.assert_equals(shipper.status().counts, { sent = 0, dead = 0, failures = 0, stale = 0 })
    t.assert_equals(shipper.status().holding, false)
    t.assert_equals(shipper.sender('a'), nil)

    -- Поколение забыто: следующий проход открывает его заново.
    configured({ a = helper.sender() })
    shipper.flush()

    t.assert_equals(it.spaces.barriers, 2)
end

--- Число ряда — после наблюдения этой загрузки: проверки соседних файлов
--- кладут в реестр ряды своей загрузки, и наши возвращает туда первое же
--- наблюдение.
---@param name string
---@param labels table|nil
---@return number|nil
local function row(name, labels)
    series.count({ sent = 0 }, 'series.probe', 'sent')

    return helper.value(name, labels)
end

g.test_a_pass_counts_what_went_and_what_was_buried_by_destination = function()
    stand({
        helper.row(1, 'series.paid', 1),
        helper.row(2, 'series.shipped', 2),
        helper.row(3, 'series.paid', 3),
    })
    configured({
        ['series.paid'] = helper.sender(),
        ['series.shipped'] = helper.sender({ { err = { retriable = false, message = 'не принято' } } }),
    })

    local report = shipper.flush()

    t.assert_equals({ report.sent, report.dead }, { 2, 1 })
    t.assert_equals(shipper.status().counts, { sent = 2, dead = 1, failures = 0, stale = 0 })
    t.assert_equals(row('outbox_sent_total', { destination = 'series.paid' }), 2)
    t.assert_equals(row('outbox_dead_total', { destination = 'series.shipped' }), 1)
    t.assert_equals(row('outbox_sent_total', { destination = 'series.shipped' }), nil)
    t.assert_equals(series.DESTINATIONS, 100)
end

g.test_the_gauges_show_the_depth_and_the_age_of_the_head = function()
    local it = stand({ helper.row(1, 'a', 1), helper.row(2, 'a', 2) })

    table.insert(it.spaces.dead, { id = 'id-0' })

    t.assert_equals(row('outbox_depth', { state = 'pending' }), 2)
    t.assert_equals(row('outbox_depth', { state = 'dead' }), 1)
    t.assert_equals(
        row('outbox_oldest_seconds'),
        5000 - 1001,
        'возраст первой строки, а не последней'
    )

    local status = shipper.status()

    t.assert_equals({ status.pending, status.dead, status.oldest_seconds }, { 2, 1, 3999 })
end

g.test_an_empty_outbox_waits_zero_seconds = function()
    stand({})

    t.assert_equals(row('outbox_depth', { state = 'pending' }), 0)
    t.assert_equals(
        row('outbox_oldest_seconds'),
        0,
        'пустой ящик — ноль, а не пропавший ряд'
    )
    t.assert_equals(shipper.status().oldest_seconds, nil)
end

g.test_a_head_from_a_clock_ahead_is_zero_seconds_old = function()
    local it = stand({ helper.row(1, 'a', 1) })

    it.realtime = 1000

    t.assert_equals(row('outbox_oldest_seconds'), 0)
    t.assert_equals(shipper.status().oldest_seconds, -1, 'сводка показывает часы как есть')
end

g.test_an_outbox_not_set_up_shows_no_gauges = function()
    local it = stand({ helper.row(1, 'a', 1) })

    function it.spaces.depth()
        return nil
    end

    t.assert_equals(row('outbox_depth', { state = 'pending' }), nil)
    t.assert_equals(row('outbox_oldest_seconds'), nil)

    local status = shipper.status()

    t.assert_equals({ status.pending, status.dead, status.oldest_seconds }, { 0, 0, nil })
end
