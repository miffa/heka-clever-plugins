
--[[
Given a Diamond message written to a log line, like:

    "servers.hostname-1.env-name.cpu.cpu0.user: 30.1"

Will write the following fields to the message:

    [diamond_hostname] = hostname-1
    [env] = env-name
    [metric] = cpu.cpu0.user
    [value] = 30.1
    [type] "gauge"

This allows for easier matching and routing.

----

Config:
- msg_type (string, optional, default "diamond_stat")
    String to set as the new message Type (prefixed by 'heka.sandbox')

*Example Heka Configuration*
.. code-block:: ini

    [DiamondStatDecoder]
    type = "SandboxFilter"
    filename = "lua/decoders/diamond_stat_decoder.lua"

        [DiamondStatDecoder.config]
        msg_type="custom_msg_type"
--]]

require "string"

local msg_type = read_config("msg_type") or "diamond_stat"

function process_message ()
    -- Assumes input is coming from Diamond logs with prefix of runtime environment
    payload = read_message("Payload")
    hostname, env, metric, value = string.match(payload, ".*servers.([a-z0-9-]+).([a-z0-9-]+).(.*): (.*)")
    value = tonumber(value)

    if hostname and env and metric and value then
        write_message("Payload", "")
        write_message("Type", msg_type)
        -- Namespace as diamond_hostname to prevent confusion with "Hostname" on Heka message
        write_message("Fields[diamond_hostname]", hostname)
        write_message("Fields[env]", env)
        write_message("Fields[metric]", metric)
        write_message("Fields[value]", value)
        write_message("Fields[type]", "gauge")
        return 0
    end

    return -1
end

