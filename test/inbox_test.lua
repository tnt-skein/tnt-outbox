--- Отметки получателя без узла: умолчания, настройки, отказы аргументов
--- на строке вызывающего и такт уборки с сорвавшимся проходом.
---
--- Отметка и уборка — работа с `box`, и они идут на настоящем узле
--- (`inbox_node_test.lua`). Здесь — то, что решается до первого обращения
--- к спейсу: оно обязано падать на строке вызывающего, а не внутри пакета.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local inbox = helper.inbox

local g = t.group('tnt.outbox.inbox')

g.before_each(helper.forget)

g.after_each(function()
    inbox.stop()
    helper.forget()
end)

--- Подменяет транзакцию вызывающего: в процессе проверок узла нет,
--- и транзакции тоже.
---@param inside boolean Идёт ли транзакция
local function in_txn(inside)
    inbox._set_source({
        in_txn = function()
            return inside
        end,
    })
end

g.test_the_defaults_of_the_marks_are_the_written_ones = function()
    -- Числа здесь свои, а не из модуля: умолчание — обещание тому, кто
    -- отметки не настраивал, и сдвинься оно, проверка обязана упасть.
    t.assert_equals(inbox.NAME, 'outbox_inbox')
    t.assert_equals(inbox.DEFAULT_RETENTION, 604800)
    t.assert_equals(inbox.DEFAULT_INTERVAL, 60)
    t.assert_equals(inbox.DEFAULT_BATCH, 1000)
end

g.test_marks_without_settings_keep_a_week_and_stand_still = function()
    t.assert_equals(inbox.status(), {
        running = false,
        retention = 604800,
        interval = 60,
        batch = 1000,
        marks = 0,
        counts = { claimed = 0, repeated = 0, removed = 0 },
    })
end

g.test_the_settings_are_given_whole_and_the_unnamed_return_to_defaults = function()
    inbox.configure({ retention = 3600, interval = 5, batch = 10 })

    local status = inbox.status()

    t.assert_equals(status.retention, 3600)
    t.assert_equals(status.interval, 5)
    t.assert_equals(status.batch, 10)

    -- Не названное теперь возвращается к умолчанию: иначе сбросить
    -- заданное прежде было бы нечем.
    inbox.configure({ retention = 60 })

    status = inbox.status()

    t.assert_equals(status.retention, 60)
    t.assert_equals(status.interval, 60)
    t.assert_equals(status.batch, 1000)

    -- Пустота из конфигурации — то же, что ключа нет.
    inbox.configure({ retention = box.NULL, batch = 5 })

    status = inbox.status()

    t.assert_equals(status.retention, 604800)
    t.assert_equals(status.batch, 5)
end

g.test_wrong_settings_are_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                inbox.configure({ nope = 1 })
            end,
            'настройки отметок: ключа «nope» нет, есть batch, interval, retention',
        },
        {
            function()
                inbox.configure({ retention = 0 })
            end,
            'настройки отметок.retention — число больше 0, а не 0',
        },
        {
            function()
                inbox.configure({ interval = 0 })
            end,
            'настройки отметок.interval — число больше 0, а не 0',
        },
        {
            function()
                inbox.configure({ batch = 0 })
            end,
            'настройки отметок.batch — число больше 0, а не 0',
        },
        {
            function()
                inbox.configure({ batch = 1.5 })
            end,
            'настройки отметок.batch — целое число, а не 1.5',
        },
    })
end

g.test_wrong_arguments_of_a_claim_are_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                inbox.claim('Чеки', { id = 'm-1' })
            end,
            'имя получателя — строка по образцу ^%a[%w_.-]*$, а не «Чеки»',
        },
        {
            function()
                inbox.claim('receipts', helper.wrong('m-1'))
            end,
            'сообщение — таблица, а не строка',
        },
        {
            function()
                inbox.claim('receipts', helper.wrong({}))
            end,
            'сообщение.id — непустая строка, а не nil',
        },
        {
            function()
                inbox.claim('receipts', helper.wrong({ id = '' }))
            end,
            'сообщение.id — непустая строка, а не пустая',
        },
    })
end

g.test_a_claim_outside_a_transaction_is_raised_at_the_caller = function()
    -- Отметка без записи обработчика либо переживёт несделанную запись,
    -- либо не успеет: защищать ей нечего.
    helper.assert_blamed({
        {
            function()
                inbox.claim('receipts', { id = 'm-1' })
            end,
            'отметку ставят в транзакции обработчика: без его записи она повтор не отсекает',
        },
    })
end

g.test_a_claim_before_the_start_is_raised_at_the_caller = function()
    in_txn(true)

    helper.assert_blamed({
        {
            function()
                inbox.claim('receipts', { id = 'm-1' })
            end,
            'отметки получателя ещё не заведены: позовите inbox.start() при подъёме узла',
        },
    })

    t.assert_equals(inbox.status().counts, { claimed = 0, repeated = 0, removed = 0 })
end

g.test_a_sweep_inside_a_transaction_is_raised_at_the_caller = function()
    in_txn(true)

    -- Уборка уступает, а уступка рвёт транзакцию вызывающего: и уборка
    -- руками, и запуск, первый проход которого идёт тут же.
    helper.assert_blamed({
        {
            function()
                inbox.sweep()
            end,
            'уборка отметок уступает и сама фиксирует куски: внутри транзакции её не зовут',
        },
        {
            function()
                inbox.start()
            end,
            'уборка отметок уступает и сама фиксирует куски: внутри транзакции её не зовут',
        },
    })

    t.assert_equals(inbox.status().running, false)
end

g.test_a_sweep_on_a_node_that_does_not_write_removes_nothing = function()
    t.assert_equals(inbox.sweep(), { removed = 0, more = false, skipped = 'узел только для чтения' })

    -- Такт на узле для чтения идёт и молчит: стирание приедет от ведущего.
    inbox.start()

    t.assert_equals(inbox.status().running, true)

    inbox.stop()

    t.assert_equals(inbox.status().running, false)
end

g.test_a_tick_that_broke_is_written_down_and_the_sweep_lives_on = function()
    inbox.configure({ interval = 0.01 })
    inbox.start()

    -- Узел «начал писать», а спейса завести не на чем: box не настроен.
    -- Такт обязан пережить это и сказать.
    inbox._set_source({
        read_only = function()
            return false
        end,
    })

    helper.until_true('о сорвавшемся такте не написано', function()
        return helper.journal.logged('такт уборки отметок не отработал')
    end)

    t.assert_equals(inbox.status().running, true)
    t.assert_str_contains(
        helper.journal.find('такт уборки отметок не отработал').record.fields.err,
        'box.cfg'
    )
end
