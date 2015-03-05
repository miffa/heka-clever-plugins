require "os"
require "string"
require "table"

local cjson = require("cjson")

-- Encodes a message to be passed to Slack, using HTTP output
-- Chat text is read from Fields[msg]
-- Config fields are optional. Read more about Slack webhooks:
-- https://api.slack.com/incoming-webhooks

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

function process_message()
    local slack_alert = {}

    -- Read config values from heka .toml
    config_fields = {"icon_emoji", "username", "channel"}
    for _, field in ipairs(config_fields) do
        val = read_config(field)
        if val then
          slack_alert[field] = val
        end
    end

    -- Read Slack message text from message Fields "msg"
    text = read_message("Fields[msg]")
    if text == nil then
        return -1
    end
    slack_alert["text"] = text

    inject_payload("json", "Slack", cjson.encode(slack_alert))
    return 0
end
