--- Фасад ящика без узла: настройки, отказы записи, сводка и такт вывоза.
---
--- Всё, что трогает спейсы, идёт на настоящем узле (`outbox_node_test.lua`).
--- Здесь — проверки аргументов, которые обязаны падать на строке вызывающего,
--- и такт, которому спейсы приходят двойником.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local outbox = helper.outbox
local shipper = helper.shipper

local g = t.group('tnt.outbox')

g.before_each(helper.forget)

g.after_each(function()
    outbox.stop()
    helper.forget()
end)

g.test_the_defaults_of_the_outbox_are_the_written_ones = function()
    -- Числа здесь свои, а не из модуля: умолчание — обещание тому, кто
    -- ящик не настраивал, и сдвинься оно, проверка обязана упасть.
    t.assert_equals(outbox.DEFAULT_INTERVAL, 1)
    t.assert_equals(outbox.DEFAULT_DEAD, 100)
    t.assert_equals(outbox.DEFAULT_KICK, 1)
    t.assert_equals(outbox.MAX_KICK, 1000)
end

g.test_an_outbox_without_settings_has_no_senders_and_stands_still = function()
    local status = outbox.status()

    t.assert_equals(status.senders, {})
    t.assert_equals(status.running, false)
    t.assert_equals(status.interval, 1)
    t.assert_equals(status.batch, shipper.DEFAULT_BATCH)
    t.assert_equals(status.counts, { written = 0, kicked = 0, sent = 0, dead = 0, failures = 0, stale = 0 })
    t.assert_equals(status.pending, 0)
    t.assert_equals(status.oldest_seconds, nil)
end

g.test_the_settings_name_the_senders_the_tick_and_the_batch = function()
    local sender = helper.sender()

    outbox.configure({
        senders = { ['order.paid'] = sender, ['order.shipped'] = sender },
        interval = 5,
        batch = 7,
        backoff = { base = 2, factor = 3, jitter = 0, max = 30 },
    })

    local status = outbox.status()

    t.assert_equals(status.senders, { 'order.paid', 'order.shipped' })
    t.assert_equals(status.interval, 5)
    t.assert_equals(status.batch, 7)
    t.assert_equals(shipper.sender('order.paid'), sender)

    -- Отправители заменяются целиком: снятый снят.
    outbox.configure({ senders = { ['order.paid'] = sender } })

    t.assert_equals(outbox.status().senders, { 'order.paid' })
    t.assert_equals(outbox.status().interval, 1)
end

g.test_a_setting_the_sender_cannot_do_is_refused_at_the_write = function()
    outbox.configure({
        senders = {
            ['order.paid'] = helper.sender(nil, { ttl = false }),
            ['order.shipped'] = helper.sender(),
        },
    })

    helper.assert_blamed({
        {
            function()
                outbox.write('order.paid', { order = 1 }, { ttl = 60 })
            end,
            'настройки записи: «ttl» — отправитель не умеет',
        },
        {
            -- Отправитель, не назвавший запретов, умеет всё: ящик доходит
            -- до спейса и отказывает уже его отсутствием.
            function()
                outbox.write('order.shipped', { order = 1 }, { ttl = 60 })
            end,
            'ящик ещё не заведён: позовите outbox.start() при подъёме узла',
        },
    })
end

g.test_wrong_settings_are_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                outbox.configure({ nope = 1 })
            end,
            'настройки ящика: ключа «nope» нет, есть backoff, batch, interval, senders',
        },
        {
            function()
                outbox.configure({ interval = 0 })
            end,
            'настройки ящика.interval — число больше 0, а не 0',
        },
        {
            function()
                outbox.configure({ batch = 0 })
            end,
            'настройки ящика.batch — число больше 0, а не 0',
        },
        {
            function()
                outbox.configure({ backoff = { nope = 1 } })
            end,
            'настройки ящика.backoff: ключа «nope» нет, есть base, factor, jitter, max',
        },
        {
            function()
                outbox.configure({ backoff = { base = -1 } })
            end,
            'настройки ящика.backoff.base — число не меньше 0, а не -1',
        },
        {
            -- Множитель меньше единицы сокращал бы паузу с каждым отказом:
            -- это не отступ, а разгон.
            function()
                outbox.configure({ backoff = { factor = 0.5 } })
            end,
            'настройки ящика.backoff.factor — число от 1 до 100, а не 0.5',
        },
        {
            function()
                outbox.configure({ backoff = { jitter = 2 } })
            end,
            'настройки ящика.backoff.jitter — число от 0 до 1, а не 2',
        },
        {
            function()
                outbox.configure({ backoff = { max = -1 } })
            end,
            'настройки ящика.backoff.max — число не меньше 0, а не -1',
        },
        {
            function()
                outbox.configure({ senders = { ['Заказы'] = helper.sender() } })
            end,
            'имя назначения — строка по образцу ^%a[%w_.-]*$, а не «Заказы»',
        },
        {
            function()
                outbox.configure({ senders = { orders = { features = {} } } })
            end,
            'отправитель orders.send — функция или вызываемая таблица, а не nil',
        },
        {
            function()
                outbox.configure({ senders = { orders = helper.wrong('шина') } })
            end,
            'отправитель orders — таблица, а не строка',
        },
        {
            function()
                outbox.configure({ senders = { orders = { send = print, features = helper.wrong(1) } } })
            end,
            'отправитель orders.features — таблица, а не число',
        },
    })
end

g.test_a_name_without_a_sender_is_raised_at_the_writer = function()
    outbox.configure({ senders = { ['order.paid'] = helper.sender() } })

    helper.assert_blamed({
        {
            function()
                outbox.write('order.shipped', { order = 1 })
            end,
            'ящику некому отправлять «order.shipped»: отправитель не назван',
        },
        {
            function()
                outbox.write('Заказы', { order = 1 })
            end,
            'имя назначения — строка по образцу ^%a[%w_.-]*$, а не «Заказы»',
        },
        {
            -- Настройки и тело проверяются до всякого обращения к спейсу.
            function()
                outbox.write('order.paid', { order = 1 }, { nope = 1 })
            end,
            'настройки записи: ключа «nope» нет, есть delay, headers, id, key, priority, timeout, ttl',
        },
        {
            function()
                outbox.write('order.paid', print)
            end,
            'тело — простые данные, а не function',
        },
        {
            -- Предел тела тот же, что у отправителя: тело, которое ящик
            -- принял, отправитель на вывозе не отвергнет.
            function()
                outbox.write('order.paid', helper.deep(126))
            end,
            'тело — вложенность глубже 100 таблиц',
        },
        {
            function()
                outbox.write('order.paid', helper.wide(10000))
            end,
            'тело — не больше 10000 значений',
        },
    })
end

g.test_writing_into_an_outbox_that_is_not_there_yet_is_raised = function()
    outbox.configure({ senders = { ['order.paid'] = helper.sender() } })

    -- Ящик заводит вывоз: заведение спейсов уступает, и запись, взявшаяся
    -- за него сама, показала бы соседу недостроенный спейс.
    helper.assert_blamed({
        {
            function()
                outbox.write('order.paid', { order = 1 })
            end,
            'ящик ещё не заведён: позовите outbox.start() при подъёме узла',
        },
    })
end

g.test_the_dead_are_asked_for_by_a_whole_positive_number = function()
    helper.assert_blamed({
        {
            function()
                outbox.dead(0)
            end,
            'сколько взять — число больше 0, а не 0',
        },
        {
            function()
                outbox.dead(helper.wrong('все'))
            end,
            'сколько взять — целое число, а не строка',
        },
    })

    -- Спейса ещё нет, и зарытых тоже.
    t.assert_equals(outbox.dead(), {})
end

g.test_the_dead_are_kicked_back_by_a_whole_positive_number_into_a_built_outbox = function()
    helper.assert_blamed({
        {
            function()
                outbox.kick(0)
            end,
            'сколько вернуть — число от 1 до 1000, а не 0',
        },
        {
            -- Возврат держит узел без уступки: больше тысячи — несколькими
            -- вызовами.
            function()
                outbox.kick(1001)
            end,
            'сколько вернуть — число от 1 до 1000, а не 1001',
        },
        {
            function()
                outbox.kick(helper.wrong('все'))
            end,
            'сколько вернуть — целое число, а не строка',
        },
        {
            -- Тысяча проходит проверку, а возвращать некуда: ящик заводит
            -- вывоз.
            function()
                outbox.kick(1000)
            end,
            'ящик ещё не заведён: позовите outbox.start() при подъёме узла',
        },
        {
            function()
                outbox.kick()
            end,
            'ящик ещё не заведён: позовите outbox.start() при подъёме узла',
        },
    })
end

g.test_a_kick_inside_a_transaction_is_raised_at_the_caller = function()
    helper.commit._set_source({
        in_txn = function()
            return true
        end,
    })

    -- Откат чужой транзакции молча вернул бы строки в зарытые, а число
    -- возвращённых вызывающий уже получил бы.
    helper.assert_blamed({
        {
            function()
                outbox.kick(1)
            end,
            'возврат зарытых идёт своей транзакцией, и в чужой его не зовут',
        },
    })
end

g.test_a_tick_that_emptied_a_full_batch_asks_for_the_next_at_once = function()
    local spaces = helper.spaces({ helper.row(1, 'a', 1), helper.row(2, 'a', 2) })

    shipper._set_source({
        space = spaces,
        read_only = function()
            return false
        end,
    })
    outbox.configure({ senders = { a = helper.sender() }, batch = 1, interval = 60 })
    outbox.start()

    t.assert_equals(outbox.status().running, true)

    -- Такт вывез кусок целиком и, не дожидаясь своего срока, взял
    -- следующий: иначе миллион строк уезжал бы куском в минуту.
    helper.until_true('ящик не опустел', function()
        return #spaces.rows == 0
    end)

    outbox.stop()

    t.assert_equals(outbox.status().running, false)
    t.assert_equals(outbox.status().counts.sent, 2)
end

g.test_a_tick_that_broke_is_written_down_and_the_relay_lives_on = function()
    local spaces = helper.spaces({})
    local passes = 0

    -- Спейсы пропали посреди работы: такт обязан пережить это и сказать.
    spaces.open = function()
        passes = passes + 1

        if passes > 1 then
            error('спейса outbox нет')
        end
    end

    shipper._set_source({
        space = spaces,
        read_only = function()
            return false
        end,
    })
    outbox.configure({ senders = { a = helper.sender() }, interval = 0.01 })
    outbox.start()

    helper.until_true('о сорвавшемся такте не написано', function()
        return helper.journal.logged('такт вывоза ящика не отработал')
    end)

    outbox.stop()

    t.assert_str_contains(
        helper.journal.find('такт вывоза ящика не отработал').record.fields.err,
        'спейса outbox нет'
    )
end

g.test_a_pass_on_a_node_that_does_not_write_ships_nothing = function()
    t.assert_equals(outbox.flush().skipped, 'узел только для чтения')
end
