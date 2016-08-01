--[[

Periodically emits a heartbeat message to syslog (console output),
then reprocesses the messages to estimate syslog-to-logparser delay.
The delay is sent to SignalFx to monitor the logging pipeline.

Config:

- ticker_interval (int, optional, defaults to 1)
- hostname (string, optional, defaults to 'unknown')
- environment (string, optional, defaults to 'unknown')

--]]

require "cjson"
require "math"
require "os"
require "table"

local hostname = read_config("hostname") or "unknown"
local environment = read_config("environment") or "unknown"

-- SignalFx metric name for logparser delay
local metric_name_delay = read_config("metric_name_delay") or "log-monitor-delay"

-- Initialize to current system time on heka start-up
local latest_heartbeat_timestamp = os.time()
local latest_heartbeat_detected = os.time()

local signalfx_fields = {
  Source = "heka",
  Title = "heartbeat-delay-for-signalfx"
}

--[[
process_message runs on logstreamer input messages that match the
emitted heartbeats from timer_event().

Each time it is called:

* Set latest_heartbeat_timestamp to the HeartbeatTimestamp field from the message.
* Set latset_heartbeat_detected to the current time (seconds since epoch).
--]]
function process_message()
  -- Time when the heartbeat message was created, in seconds since epoch
  -- We need to floor because the HeartbeatTimestamp has nanosecond precision,
  -- while os.time() is only seconds. This ensures the delay is always >= 0s.
  -- i.e. 1469730290.12345 heartbeat and 1469730290 current = delay of -0.12345s
  local msg_time = math.floor(tonumber(read_message("Fields[HeartbeatTimestamp]")))

  -- Current system time, in seconds since epoch
  -- local current_time = os.time()
  latest_heartbeat_timestamp = msg_time
  latest_heartbeat_detected = os.time()

  return 0
end

--[[
timer_event runs at the interval specified by ticker_interval in the heka config.

Each time it is called:

* Emit a new heartbeat message marked with the current time.

* Compute and the msot recent delay value to SignalFx.
  This should be a multiple of the ticker_interval. For example, for a 15s ticker,
  the delay will usually be 15s, or higher if there are delays in heka processing.

--]]
function timer_event(ns)
  -- ========== Heartbeat message ==========

  -- Fields are used to match the heartbeats in the heka config.
  local heartbeat_fields = {
    Source = "heka",
    Title = "heartbeat"
  }
  local payload = {
    -- divide by 1e9 to send the time in epoch seconds
    HeartbeatTimestamp = ns/1e9,
    Source = "heka",
    Title = "heartbeat"
  }
  local message = {
    Fields = heartbeat_fields,
    Payload = cjson.encode(payload)
  }
  inject_message(message)

  -- ========== SignalFx message ==========

  -- The latest_delay value is typicall the delay from the _previous_ heartbeat,
  -- not the one sent immediately above in this same timer event.
  local output = {}
  local gauges = {}

  local datapoint = {
    metric = metric_name_delay,
    value = latest_heartbeat_detected - latest_heartbeat_timestamp,
    dimensions = {hostname=hostname, environment=environment}
  }
  table.insert(gauges, datapoint)
  output.gauge = gauges

  local message = {
    Fields = signalfx_fields,
    Payload = cjson.encode(output)
  }
  inject_message(message)
end
