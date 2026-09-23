--- Строка ящика: что ляжет в спейс.
---
--- Настройки записи и тело проверяет общая часть договора очереди; здесь —
--- то, что ящик собирает сам.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local message = helper.message

local g = t.group('tnt.outbox.message')

g.test_the_record_carries_the_identifier_and_the_headers_of_the_writer = function()
    local record = context.run({ request_id = 'r-9' }, function()
        return message.record({ order = 11 }, { key = 'o-11', delay = 5, ttl = 60, priority = 1, timeout = 3 })
    end)

    t.assert_equals(record.body, { order = 11 })
    t.assert_equals(#record.options.id, 26)
    t.assert_equals(record.options.headers['x-request-id'], 'r-9')
    t.assert_equals(record.options.key, 'o-11')
    t.assert_equals(record.options.delay, 5)
    t.assert_equals(record.options.ttl, 60)
    t.assert_equals(record.options.priority, 1)
    t.assert_equals(record.options.timeout, 3)
end

g.test_a_ready_identifier_and_own_headers_win = function()
    local record = context.run({ request_id = 'r-1' }, function()
        return message.record(nil, {
            id = 'ready',
            headers = { ['x-request-id'] = 'r-2', ['x-tenant'] = 'acme' },
        })
    end)

    t.assert_equals(record.options.id, 'ready')
    t.assert_equals(record.options.headers['x-request-id'], 'r-2')
    t.assert_equals(record.options.headers['x-tenant'], 'acme')
    t.assert_equals(record.body, nil)
end
