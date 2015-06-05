--[=[

Extracts data from Fields in messages.
Generates JSON suitable for send to datapoint SignalFx API.

Batches multiple messages into one string, which can be passed to an HttpOutput
using a PayloadEncoder.

(Currently, only supports writing data points, NOT events.)

Config:

- metric_name (string, required)
    String to use as the `metric` name in SignalFX.

    Supports interpolation of field values from the processed message, using
    `%{fieldname}`.

    Any `fieldname` values of "Type", "Payload", "Hostname",
    "Pid", "Logger", "Severity", or "EnvVersion" will be extracted from the
    the base message schema, any other values will be assumed to refer to a
    dynamic message field.

    Only the first value of the first instance of a dynamic message field can be
    used for series name interpolation. If the dynamic field doesn't exist, the
    uninterpolated value will be left in the series name.

    Note that it is not possible to interpolate either the
    "Timestamp" or the "Uuid" message fields into the series name, those
    values will be interpreted as referring to dynamic message fields.

- value_field (string, optional, defaults to "value")
    The `fieldname` to use as the value for the metric in signalfx. If the `value`
    field is not present this encoder will set one as the value for counters: `1`.
    A value of `0` will be used for `gauges`.

- msg_type (string, optional, defaults to "signalfxbatch")
    `Type` of the message outputted from this filter.

- dimensions (string, optional, defaults to "")
    A space delimited list of field names. Each of these will be written as
    "dimensions" on the SignalFx data point, which you can filter by in SignalFx.
    For example, a value of "Hostname Severity" would write the field

        "dimensions": {
            "Hostname": "my-hostname.example.com",
            "Severity": 0
        }

    for the datapoint.


- max_count (int, optional, defaults to 20)
    Max number of messages before a batch is flushed from the filter.

*Example Heka Configuration*

.. code-block:: ini

    [SignalfxBatchFilter]
    message_matcher = "Fields[title] == 'your-metric' && Fields[metric_value] != NIL"
    type = "SandboxFilter"
    script_type = "lua"
    filename = "lua/filters/signalfxbatch.lua"
       [SignalfxBatchFilter.config]
       metric_name = "test-metric.%{title}.%{Hostname}"
       value_field = "metric_value"
       dimensions = "Hostname Severity"
       max_count = 5

    [SignalfxHttpOutput]
    message_matcher = "Fields[payload_name] == 'signalfxbatch'"
    type = "HttpOutput"
    encoder = "PayloadEncoder"
    address = "https://ingest.signalfx.com/v2/datapoint"
      [signalfx.headers]
      content-type = ["application/json"]
      X-SF-Token = "<YOUR-SIGNALFX-API-TOKEN>"

--]=]

require "cjson"
require "string"
require "table"

local metric_name = read_config("metric_name")
local value_field = read_config("value_field") or "value"
local dimensions_str = read_config("dimensions") or ""
local msg_type = read_config("msg_type") or "signalfxbatch"
local batch_max_count = read_config("max_count") or 20

local use_subs
if string.find(metric_name, "%%{[%w%p]-}") then
    use_subs = true
end

local base_fields_map = {
    Type = true,
    Payload = true,
    Hostname = true,
    Pid = true,
    Logger = true,
    Severity = true,
    EnvVersion = true
}

-- Allows interpolating message fields into a string.
-- Used to make custom metric names.
local function sub_func(key)
    if base_fields_map[key] then
        return read_message(key)
    else
        local val = read_message("Fields["..key.."]")
        if val then
            return val end
        return "%{"..key.."}"
    end
end

local function get_dimensions(s)
  local dims = {}
  -- TODO: make sure matcher supports all possible field names
  for i in string.gmatch(s, "%S+") do
    dims[i] = tostring(sub_func(i))
  end
  return dims
end

local counters = {}
local gauges = {}

function flush()
   if #counters > 0 or #gauges > 0 then
      local output = {}
      -- Only add to output if > 0, else Lua adds empty dict {} instead of empty array []
      if #counters > 0 then output.counter = counters end
      if #gauges > 0 then output.gauge = gauges end

      inject_payload("json", msg_type, cjson.encode(output))
      counters = {}
      gauges = {}
   end
   return 0
end

function process_message()
    local ts = tonumber(read_message("Timestamp"))
    if not ts then return -1 end
    ts = ts / 1e6 -- Convert nanoseconds to milliseconds

    local value = read_message("Fields["..value_field.."]")

    -- assume stat_type is a counter unless gauge is specified
    local stat_type = read_message("Fields[type]")
    if stat_type == "gauge" then
        if not value then return -1 end
    else
        stat_type = "counter"
        if not value then value = 1 end -- default counter to 1
    end

    -- only process name if everything looks good
    local name = ""
    if use_subs then
        name = string.gsub(metric_name, "%%{([%w%p]-)}", sub_func)
    else
        name = metric_name
    end
    if not name or name == "" then return -1 end

    -- Read custom dimensions from message
    local dims = get_dimensions(dimensions_str)

    -- single data point
    local datum = {
        metric=name,
        value=value,
        timestamp=ts,
        dimensions=dims
    }

    -- Batch data points, grouping by 'counter' or 'gauge'
    if stat_type == 'counter' then
        table.insert(counters, datum)
    elseif stat_type == 'gauge' then
        table.insert(gauges, datum)
    else
        return -1
    end

    if #counters + #gauges == batch_max_count then
        flush()
    end
    return 0
end

function timer_event(ns)
    flush()
end
