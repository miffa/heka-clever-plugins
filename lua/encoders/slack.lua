require "os"
require "string"
require "table"

local cjson = require("cjson")
--[=[
-- Encodes a message to be passed to Slack, using HTTP output
-- Chat text is read from Fields[text_field]
-- Config fields are optional. Read more about Slack webhooks:
-- https://api.slack.com/incoming-webhooks

-- Config:

- text_field (string, optional, default 'Payload')
    Field used as the message text sent to Slack. Is ignored if 
    the config `text` is set.

- text (string, optional, default nil)
    String used as the message text sent to Slack. Overrides `text_field`
    Supports :ref:`message field interpolation<sandbox_msg_interpolate_module>`.
    `%{fieldname}`. Any `fieldname` values of "Type", "Payload", "Hostname",
    "Pid", "Logger", "Severity", or "EnvVersion" will be extracted from the
    the base message schema, any other values will be assumed to refer to a
    dynamic message field.

- username (string, optional, default nil) 
    String used as the username sent to Slack

- channel (string, optional, default nil) 
    String used as the channel sent to Slack

- icon_emoji (string, optional, default nil) 
    String used as the icon_emoji sent to Slack

-- [SlackEncoder]
-- type = "SandboxEncoder"
-- script_type = "lua"
-- filename = "slack_encoder.lua"

--   [SlackEncoder.config]
--   username = "<USERNAME>"
--   channel = "<CHANNEL>"
--   icon_emoji = "<ICON_EMOJI>"

-- [HttpOutput]
-- address = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
-- message_matcher = "<MESSAGE_MATCHER>"
-- encoder = "SlackEncoder"
--]=]

local interp = require "msg_interpolate"

-- cache config values
local base_alert = {}
for _, field in ipairs({"icon_emoji", "username", "channel"}) do
    val = read_config(field) or nil
    if val then
        base_alert[field] = val
    end
end

local msg = read_config("text") or nil
local text_field = read_config("text_field") or nil

-- Cache whether or not msg is interpolated
local interp_msg = false
if msg and string.find(msg, "%%{[%w%p]-}") then
    interp_msg = true
end

function format_msg()
    local text = nil
    if interp_msg then
        text = interp.interpolate_from_msg(msg)
    else
        text = msg
    end
    return text
end

function process_message()
    local slack_alert = base_alert

    -- Read Slack message text from message Payload
    -- or from a Field set via config item `text_field`
    local text = nil
    if msg then
        text = format_msg()
    else 
        if text_field then
            text = read_message("Fields[" .. text_field .. "]")
        else
            text = read_message("Payload")
        end
    end
    if text == nil then
        return -1
    end
    slack_alert["text"] = text

    inject_payload("json", "Slack", cjson.encode(slack_alert))
    return 0
end
