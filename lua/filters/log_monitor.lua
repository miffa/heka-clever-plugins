--[[

Reports the timestamp from the latest message seen.


Config:

- ticker_interval (int, optional, defaults to 1)
- msg_type (string, optional, defaults to 'log-monitor')
- metric_name (string, optional, defaults to 'log-monitor')
- hostname (string, optional, defaults to 'unknown')
    Set the hostname (or container name) 

*Example Heka Configuration*

.. code-block:: ini

    [LastSeenTimestamp]
    message_matcher = "Logger == 'LogstreamerInput'"
    type = "SandboxFilter"
    script_type = "lua"
    filename = "lua/filters/timestamp.lua"
    ticker_interval = 1
		[LastSeenTimestamp.config]
		msg_type = "log-monitor"
		metric_name = "log-monitor"
		hostname = "HekaParser"


--]]

require "cjson"
require "string"
require "table"

local msg_type = read_config("msg_type") or "log-monitor"
local metric_name = read_config("metric_name") or "log-monitor"
local hostname = read_config("hostname") or "unknown"
local last_timestamp = 0

function process_message()
    local ts = tonumber(read_message("Timestamp"))
    -- convert from Unix nanoseconds to Unix seconds
    last_timestamp = ts / 1e9
    return 0
end

function timer_event(ns)
    local output = {}
    local gauges = {}
    local datapoint = {
    	metric=metric_name,
    	value=last_timestamp,
    	dimensions={hostname=hostname} 
    }
    table.insert(gauges, datapoint)
    output.gauge = gauges
    inject_payload("json", msg_type, cjson.encode(output))
end
