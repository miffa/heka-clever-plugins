--[[

Counterfilter to Fields

This field extracts values from CounterFilter's output and write's them as Fields on the Heka message.
This allows for easier matching and routing.

Note that CounterFilter emits two different kinds of messages:

1. every interval, says # of total messages and # of messages/sec
2. every (larger) interval, rolls up this data into aggregate stats

This plugin extracts fields from type (1).

Config:

- msg_type (string, optional, default "counterfilter-to-fields")
    String to set as the new message Type (automatically gets prefixed by 'heka.sandbox')

*Example Heka Configuration*

.. code-block:: ini

    [CounterfilterToFields]
    type = "SandboxFilter"
    filename = "lua/filters/counterfilter_to_fields.lua"

        [CounterfilterToFields.config]
        msg_type = "custom_type"

--]]

require "string"

local msg_type     = read_config("msg_type") or "counterfilter-to-fields"

function process_message ()

    local payload = read_message("Payload")
    local message = {
      Timestamp  = read_message("Timestamp"),
      Type       = msg_type,
      EnvVersion = read_message("EnvVersion"),
      Fields     = {},
      Payload    = payload
    }

    -- Example payload: "Got 5 messages. 5.3 msg/sec"
    local total, per_sec = string.match(payload, "Got ([0-9]+) messages. ([.0-9]+) msg/sec")
    total = tonumber(total)
    per_sec = tonumber(per_sec)
    if not total or not per_sec then
      return -1
    end

    message.Fields['messages_total'] = total
    message.Fields['messages_per_sec'] = per_sec

    inject_message(message)
    return 0
end
