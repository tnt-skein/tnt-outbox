--- Что ящик кладёт в строку и чем потом зовёт отправителя.
---
--- **Опознаватель и заголовки ставит запись, а не вывоз**: ULID `tnt-id`
--- и `context.export()` берутся в миг `write` и ложатся в строку. Поэтому
--- повторный вывоз несёт тот же опознаватель, а `x-request-id` и трасса
--- у отправителя — те, что были у записи, а не у такта вывоза.
---
--- **Конверт сообщения собирает отправитель**, а не ящик: ящик зовёт
--- `send(name, body, opts)` назначенного отправителя, и `id`, `headers`,
--- `key` приходят к нему настройками — отдать готовый конверт этим вызовом
--- нечем. Отсюда одно отступление: время отправки у получателя — миг
--- вывоза, а не записи. Когда сообщение родилось, говорят старшие биты
--- его ULID, а сколько оно пролежало в ящике — `oldest_seconds` сводки.
---
--- Настройки записи и тело проверяет общая часть договора очереди
--- до того, как сюда дойдёт: это настройки `send` назначенного отправителя
--- и его тело, и правило у них то же, что у самого отправителя.

local id = require('tnt.id')
local message = require('tnt.message')

local Module = {}

---@class TntOutboxSendOptions : TntMessageSendOptions Настройки, готовые к отправке
---@field id string Опознаватель: он есть всегда — свой ULID либо готовый
---@field headers table<string, string> Заголовки: контекст записи и свои

---@class TntOutboxRecord Строка ящика: тело и настройки его отправки
---@field body any Тело — простые данные
---@field options TntOutboxSendOptions Настройки с опознавателем и заголовками

--- Собирает то, что ляжет в строку, в миг записи.
---@param body any Тело
---@param opts TntMessageSendOptions Проверенные настройки
---@return TntOutboxRecord
function Module.record(body, opts)
    local options = {
        id = opts.id or id.ulid(),
        headers = message.headers(opts.headers),
        key = opts.key,
        delay = opts.delay,
        ttl = opts.ttl,
        priority = opts.priority,
        timeout = opts.timeout,
    }

    return { body = body, options = options }
end

return Module
