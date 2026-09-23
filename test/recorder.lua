--- Двойник отправителя для проверок ящика: помнит, что ему отдали,
--- и отвечает по заготовленному сценарию.
---
--- Модулем, а не замыканием в каждой проверке: половина проверок ящика идёт
--- на узле, а тело `server:exec` уезжает туда без замыканий — двойник
--- пришлось бы объявлять в каждой заново, и они разъехались бы между собой.

local Module = {}

---@class TntOutboxTestAnswer Что двойник ответит на очередную отправку
---@field id string|nil Опознаватель: пустота — отказ
---@field err any Отказ второго значения
---@field raise any Что бросить вместо ответа

--- Собирает отправителя.
---
--- Ответ по умолчанию — согласие: двойник отдаёт тот опознаватель, который
--- ему принесли, и по нему видно, что вывоз везёт сохранённое, а не своё.
---@param answers TntOutboxTestAnswer[]|nil Ответы по порядку отправок
---@param features table<string, boolean>|nil Что двойник умеет
---@return table sender
function Module.new(answers, features)
    local sender = { sent = {}, bodies = {}, features = features or {} }

    function sender.send(name, body, opts)
        table.insert(sender.sent, { name = name, body = body, opts = opts })

        local answer = answers ~= nil and answers[#sender.sent] or nil

        if answer == nil then
            table.insert(sender.bodies, body)

            return opts.id
        end

        if answer.raise ~= nil then
            error(answer.raise, 0)
        end

        if answer.id ~= nil then
            table.insert(sender.bodies, body)
        end

        return answer.id, answer.err
    end

    return sender
end

return Module
