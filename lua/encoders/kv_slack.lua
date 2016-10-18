--[=[
Encodes a message to be passed to Slack, using HTTP output.
Config fields are required.

Read more about Slack webhooks:
https://api.slack.com/incoming-webhooks

Config:

- message_field (string)
    Field containing the message sent to Slack.

- username_field (string)
    Field containing the username sent to Slack.

- channel_field (string)
    Field containing the channel sent to Slack.

- icon_field (string)
    Field containing the icon (emoji) sent to Slack.

*Example Heka Configuration*

.. code-block:: ini

    [SlackEncoder]
    type = "SandboxEncoder"
    script_type = "lua"
    filename = "slack_encoder.lua"

      [SlackEncoder.config]
      message_field = "<MESSAGE_FIELD>"
      username_field = "<USERNAME_FIELD>"
      channel = "<CHANNEL_FIELD>"
      icon_field = "<ICON_FIELD>"

    [HttpOutput]
    address = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    message_matcher = "<MESSAGE_MATCHER>"
    encoder = "SlackEncoder"
--]=]

local cjson = require "cjson"
local field_util = require "field_util"

local config = {
    text = read_config("message_field") or error("message_field must be set in config"),
    username = read_config("username_field") or error("username_field must be set in config"),
    channel = read_config("channel_field") or error("channel_field must be set in config"),
    icon_emoji = read_config("icon_field") or error("icon_field must be set in config"),
}

local base_fields = field_util.field_map()
base_fields['Timestamp'] = true

-- `read_field` gets the value for a field.
-- Routes to appropriate lookup for Heka internal fields (see `base_fields`)
-- or custom message fields.
local function read_field(key)
    if not key then return nil end

    if base_fields[key] then
        return read_message(key)
    else
        return read_message("Fields["..key.."]")
    end
end

function process_message()
    local slack_alert = {}
    for _, field in ipairs({"text", "icon_emoji", "username", "channel"}) do
        local val = read_field(config[field])
        slack_alert[field] = val
    end

    inject_payload("json", "KvSlack", cjson.encode(slack_alert))
    return 0
end
