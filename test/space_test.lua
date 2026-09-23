--- Спейсы ящика без узла: пока их нет, движение отказывает броском,
--- а сводка честно говорит «ничего не знаю».
---
--- Сами спейсы — последовательность ключей, зарытые и барьер — проверяются
--- на узле (`outbox_node_test.lua`): заводить их в процессе проверок
--- негде, `box.cfg` там не звался.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local space = helper.space

local g = t.group('tnt.outbox.space')

g.test_without_a_node_there_is_no_outbox = function()
    t.assert_equals(space.get(), nil)
    t.assert_equals(space.first(), nil)
    t.assert_equals(space.depth(), nil)
    t.assert_equals(space.dead(10), {})
end

g.test_a_move_without_the_outbox_is_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                space.head(10)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.put('id', 'orders', { body = 1, options = {} }, 0)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.remove(1)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.top()
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.barrier(0)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.bury({ key = 1, id = 'id', name = 'orders' }, 'причина', 0)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
        {
            function()
                space.revive(10, 0)
            end,
            'спейса outbox нет: ящик заводит outbox.start()',
        },
    })
end
