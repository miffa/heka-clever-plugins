--[=[

Extracts data from Fields in messages.
Generates JSON suitable for send to datapoint SignalFx API.
https://developers.signalfx.com/docs/datapoint

Batches multiple messages into one string, which can be passed to an HttpOutput
using a PayloadEncoder.

This filter differs from the previous SignalfxBatchFilter because it pulls metadata
such as the metirc name, dimensions list, and more from the message itself.
This works in conjuction with Kayvee, which injects routing information into the message.

Config:

- dimensions_field (string, required)
    Field name from the message. The field should contain a space delimited list of
    field names. Each of these will be written as "dimensions" on the SignalFx data
    point, which you can filter by in SignalFx.

    For example, if the received message had a value of `field_a field_b", and
    corresponding fields `field_a: a` and `field_b: b`, then dimensions would look
    like:

        "dimensions": {
            "field_a": "a",
            "field_b": "b"
        }

    These dimensions may NOT be Heka internal fields.

- default_dimensions (string, optional)
    A space delimited list of field names. Each of these will be written as
    "dimensions" on the SignalFx data point, in addition to the fields set via
    `dimensions_field`.

    These dimensions may be Heka internal fields.

- msg_type (string, optional, defaults to "kayvee_signalfxbatch")
    `Type` of the message outputted from this filter.

- max_count (int, optional, defaults to 20)
    Max number of messages before a batch is flushed from the filter.

- series_field (string, required)
    Field name, which contains a string. The value of this field will be used
    as the metric name in SignalFX.

- value_ref_field (string, required)
    The fieldname which contains the value for the metric. For example, if a message had these fields

        ```
        :Fields:
            | name:"value_field"    type:string     value:"val"
            | name:"val"            type:double     value:5
        ```

    then setting "value_ref_field" = "value_field" would result in a value of 5 written to the metric.

*Example Heka Configuration*

.. code-block:: ini


    [KayveeSignalfxBatchFilter]
    message_matcher = "TRUE"
    type = "SandboxFilter"
    script_type = "lua"
    filename = "lua/filters/kayvee_signalfxbatch_messsage.lua"
    ticker_interval = 60

        [KayveeSignalfxBatchFilter.config]
        series_field="series_f"
        value_ref_field="value_f"
        stat_type_field="stat_type_f"
        dimensions_field="dimensions_f"
        default_dimensions="Hostname"
        max_count = 1000

    [SignalfxHttpOutput]
    message_matcher = "Fields[payload_name] == 'kayvee_signalfxbatch'"
    type = "HttpOutput"
    encoder = "PayloadEncoder"
    address = "https://ingest.signalfx.com/v2/datapoint"
      [SignalfxHttpOutput.headers]
      content-type = ["application/json"]
      X-SF-Token = ["%ENV[SIGNALFX_API_TOKEN]"]

--]=]

local cjson = require "cjson"
require "string"
require "table"
require "math"

local config = {
    -- Read these values dynamically, depending on message field value
    --  * series
    --  * value
    --  * dimensions (also adds field values from default_dimensions)
    --  * stat_type
    series_field = read_config("series_field") or error("series_field must be specified"),
    value_ref_field = read_config("value_ref_field") or error("value_ref_field must be specified"),
    dimensions_field = read_config("dimensions_field") or error("dimensions_field must be specified") ,
    stat_type_field = read_config("stat_type_field") or error("stat_type_field must be specified"),
    default_dimensions = read_config("default_dimensions") or error("default_dimensions must be specified") ,
    msg_type = read_config("msg_type") or "kayvee_signalfxbatch",
    batch_max_count = read_config("max_count") or 20,
}

local base_fields_map = {
    Type = true,
    Payload = true,
    Hostname = true,
    Pid = true,
    Logger = true,
    Severity = true,
    EnvVersion = true
}

-- `read_field` gets the value for a field.
-- Routes to appropriate lookup for Heka internal fields (see `base_fields_map`)
-- or custom message fields.
local function read_field(key)
    if not key then return nil end

    if base_fields_map[key] then
        return read_message(key)
    else
        return read_message("Fields["..key.."]")
    end
end

local function get_dimensions(s)
    if s == nil then return {} end
    local dims = {}
    -- TODO: make sure matcher supports all possible field names
    for i in string.gmatch(s, "%S+") do
        local val = read_field(i)
        if val ~= nil then
            dims[i] = tostring(val)
        end
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

      inject_payload("json", config.msg_type, cjson.encode(output))
      counters = {}
      gauges = {}
   end
   return 0
end

--------------------------------
--
--  Public interface
--
--------------------------------

function process_message()
    -- Get timestamp (in milliseconds)
    local ts = tonumber(read_message("Timestamp"))
    if not ts then return -1 end
    ts = math.floor(ts / 1e6) -- Convert nanoseconds to milliseconds

    -- Get value
    local value = read_field(read_field(config.value_ref_field))

    -- Get stat_type
    local stat_type = read_field(config.stat_type_field)
    if stat_type == "gauge" then
        if not value then value = 0 end -- default gauge to 0
    elseif stat_type == "counter" then
        if not value then value = 1 end -- default counter to 1
    else
        return -1 -- error if invalid stat_type
    end

    ---- Get series
    local series = read_field(config.series_field)
    if not series then return -1 end

    ---- Read custom dimensions from message
    local dimensions_str = read_field(config.dimensions_field)
    local dims = get_dimensions(dimensions_str)

    -- Read default dimensions from message
    local default_dims = get_dimensions(config.default_dimensions)
    for k, v in pairs(default_dims) do
        dims[k] = v
    end

    -- single data point
    local datum = {
        metric=series,
        value=value,
        timestamp=ts,
        dimensions=dims
    }

    ---- Batch data points, grouping by 'counter' or 'gauge'
    if stat_type == 'counter' then
        table.insert(counters, datum)
    elseif stat_type == 'gauge' then
        table.insert(gauges, datum)
    else
        return -1
    end

    if #counters + #gauges == config.batch_max_count then
        flush()
    end
    return 0
end

function timer_event(ns)
    flush()
end
