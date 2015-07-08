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

- skip_fields (string, optional, default "")
    Space delimited set of fields that should *not* be included in the
    Signalfx dimensions being generated.

    For example, if you include all default fieldnames, your dimensions might
    look like:

        "dimensions": {
            "Hostname": "my-hostname.example.com",
            "Severity": "0",
            ...
        }

    Note: all dimensions are written as strings to SignalFx, which is required
    by their API.

    Fieldname values of "Type", "Payload", "Hostname", "Pid", "Logger", "Severity", or "EnvVersion"
    will be assumed to refer to the corresponding field from the base message
    schema, any other values will be assumed to refer to a dynamic message
    field.

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
       skip_fields = "Hostname Severity"
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
require "math"

local metric_name = read_config("metric_name")
local value_field = read_config("value_field") or "value"
local msg_type = read_config("msg_type") or "signalfxbatch"
local batch_max_count = read_config("max_count") or 20
local skip_fields_str = read_config("skip_fields") or ""

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

-- Remove blacklisted fields from the set of base fields that we use, and
-- create a table of dynamic fields to skip.
local skip_fields_str = read_config("skip_fields")

local function get_field_values(skip_fields_str)
    -- returns a table of key-val mappings
    -- i.e. { "field": 1, "field2", "other value", ... }
    --
    -- `skip_fields_str` is a space delimited set of fields that should *not* be included

    output = {}

    -- Determine fields to skip
    local skip_fields = {}
    if skip_fields_str then
        for field in string.gmatch(skip_fields_str, "[%S]+") do
            skip_fields[field] = true
        end
    end

    -- Read Heka base fields
    for base_field, _ in pairs(base_fields_map) do
        -- Skip fields specified by user
        if not skip_fields[base_field] then
            output[base_field] = read_message(base_field)
        end
    end

    -- Read Heka non-base fields
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end -- No more fields

        if typ ~= 1 then -- exclude bytes
            -- Skip fields specified by user
            if not skip_fields[name] then
                output[name] = value
            end
        end
    end

    return output
end

local function get_dimensions(keyvals)
    output = {}
    for k, v in pairs(keyvals) do
        output[k] = tostring(v)
    end

    return output
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
    ts = math.floor(ts / 1e6) -- Convert nanoseconds to milliseconds

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

    -- Read specific fields from message from message
    local field_values = get_field_values(skip_fields_str)

    -- Convert fields to valid SignalFx dimensions
    local dims = get_dimensions(field_values)

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
