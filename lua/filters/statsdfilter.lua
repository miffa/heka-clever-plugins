--[[
Given a message emitted from StatdsAccum, with multiple fields formatted like:

    "stats.gauges.servers.hostname-1.env-name.cpu.cpu0.user"

Pulls out hostname and env into separate fields.
Writes each stat like "cpu.cpu0.user" to a field of that name.

The reason for this is to:
- remove dynamic parts of the field names for easier matching
- allow easier routing based on environment

----

Config:
- msg_type (string, optional, default "statsdfilter")
    String to set as the new message Type (prefixed by 'heka.sandbox')

*Example Heka Configuration*
.. code-block:: ini

    [StatsdFilter]
    type = "SandboxFilter"
    filename = "filters/statsdfilter.lua"

--]]

require "string"

local msg_type     = read_config("msg_type") or "statsdfilter"
local payload_keep = read_config("payload_keep")


function process_message ()

    local message = {
      Timestamp  = read_message("Timestamp"),
      Type       = msg_type,
      EnvVersion = read_message("EnvVersion"),
      Fields     = {}
    }
    if payload_keep then message.Payload = read_message("Payload") end

    local hostname = nil
    local env = nil
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        -- Pull hostname and env from Statsd message
        -- Example: "stats.gauges.servers.hostname-1.env-name.cpu.cpu0.user"
        hostname, env, metric = string.match(name, "stats.gauges.servers.([a-z0-9-]+).([a-z0-9-]+).(.*)")
        if hostname and metric and env then
          message.Fields["hostname"] = hostname
          message.Fields["env"] = env
          message.Fields[metric] = value
        end
    end

    -- TODO: Inject a message for each stat
    -- TODO: Only inject if size(Fields) > 0
    inject_message(message)
    return 0
end

function timer_event(ns)
end
