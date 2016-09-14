--[=[

Converts full Heka message contents to line protocol for InfluxDB HTTP write API
(new in InfluxDB v0.9.0). This filter converts each Heka message into one InfluxDB
line and seperates multiple lines with `\n`. The `name` is re-evaluated for each line.

Generates batches suitable for send to InfluxDB API.

Batches multiple messages into one string, which can be passed to an HttpOutput
using a PayloadEncoder.

This filter differs from the previous InfluxDBLineBatch filter in that it pulls metadata
such as the series name, tags list, and more from the message itself.
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

- decimal_precision (string, optional, default "6")
    String that is used in the string.format function to define the number
    of digits printed after the decimal in number values.  The string formatting
    of numbers is forced to print with floating points because InfluxDB will
    reject values that change from integers to floats and vice-versa.  By
    forcing all numbers to floats, we ensure that InfluxDB will always
    accept our numerical values, regardless of the initial format.

- msg_type (string, optional, defaults to "signalfxbatch")
    `Type` of the message outputted from this filter.

- max_count (int, optional, defaults to 20)
    Max number of messages before a batch is flushed from the filter.

- series_field (string, required)
    Field name, which contains a string. The value of this field will be used
    as the `metric` name in SignalFX.

- timestamp_precision (string, optional, default "ms")
    Specify the timestamp precision that you want the event sent with.  The
    default is to use milliseconds by dividing the Heka message timestamp
    by 1e6, but this math can be altered by specifying one of the precision
    values supported by the InfluxDB write API (ms, s, m, h). Other precisions
    supported by InfluxDB of n and u are not yet supported.


*Example Heka Configuration*

.. code-block:: ini

    [KayveeInfluxdbLineBatchFilter]
    message_matcher = "TRUE"
    type = "SandboxFilter"
    script_type = "lua"
    filename = "lua/filters/kayvee_signalfxbatch_messsage.lua"
    can_exit = false
    ticker_interval = 60

        [KayveeInfluxdbLineBatchFilter.config]
        series_field="series_f"
        value_field="value_f"
        stat_type_field="stat_type_f"
        dimensions_field="dimensions_f"
        default_dimensions="Hostname"
        max_count = 1000

    [InfluxdbLineOutput]
    message_matcher = "Fields[payload_name] == 'influxdblinebatch'"
    encoder = "PayloadEncoder"
    type = "HttpOutput"
    address = "%ENV[INFLUXDB_LINE_PROTO]://%ENV[INFLUXDB_LINE_HOST]:%ENV[INFLUXDB_LINE_PORT]/write?db=%ENV[INFLUXDB_LINE_DB]&precision=ms"
    username = "%ENV[INFLUXDB_LINE_USER]"
    password = "%ENV[INFLUXDB_LINE_PASS]"
    http_timeout = 60000
--]=]

local field_util = require "field_util"

--------------------------------
--
--  Private Interface
--
--------------------------------

local config = {
    series_field = read_config("series_field") or error("series_field must be specified"),
    value_field = read_config("value_field") or error("value_field must be specified"),
    dimensions_field = read_config("dimensions_field") or error("dimensions_field must be specified") ,
    default_dimensions = read_config("default_dimensions") or error("default_dimensions must be specified") ,

    decimal_precision = read_config("decimal_precision") or "6",
    timestamp_precision = read_config("timestamp_precision") or "ms",
    payload_name = read_config("payload_name") or "influxdblinebatch",
    batch_max_count = read_config("max_count") or 20,
}

-- TODO: Refactor shared items
local base_fields_map = {
    Type = true,
    Payload = true,
    Hostname = true,
    Pid = true,
    Logger = true,
    Severity = true,
    EnvVersion = true
}

-- read_field gets the value for a field.
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

local function lookup_field_then_value(key)
    if not key then return nil end

    -- Get field name
    local field_name = read_message("Fields["..key.."]")
    if not field_name or field_name == "" then return nil end

    -- Get field value
    return read_field(field_name)
end

local function get_dimensions(s)
    local dims = {}
    for i in string.gmatch(s, "%S+") do
        dims[i] = read_field(i)
    end
    return dims
end

local function escape_string(str)
    return tostring(str):gsub("([ ,])", "\\%1")
end

local function encode_scalar_value(value, decimal_precision)
    if type(value) == "number" then
        -- Always send numbers as formatted floats, so InfluxDB will accept
        -- them if they happen to change from ints to floats between
        -- points in time.  Forcing them to always be floats avoids this.
        return string.format("%."..tostring(decimal_precision).."f", value)
    elseif type(value) == "string" then
        -- first unescape `"` and then escape `\`
        value_dec = tostring(value):gsub('\\"', '"'):gsub('\\', '\\\\')
        -- string values need to be double quoted. and escape `"`
        return '"' .. value_dec:gsub('"', '\\"') .. '"'
    elseif type(value) == "boolean" then
        -- don't quote booleans
        return tostring(value)
    end
end

local function sorted_keys(map)
    -- TODO: @n Do full alpha sorting instead of (A-Za-z). case sensitive currently.
    --      depends on locale... http://lua-users.org/lists/lua-l/2009-12/msg00658.html
    sorted = {}
    for key in pairs(map) do table.insert(sorted, key) end
    table.sort(sorted)
    return sorted
end

local function encode_fields(fields, decimal_precision)
    local values = {}
    if fields == nil then return values end

    for _,k in ipairs(sorted_keys(fields)) do
        v = fields[k]
        table.insert(
            values,
            string.format("%s=%s", escape_string(k), encode_scalar_value(v, decimal_precision))
        )
    end

    return values
end

local function encode_tags(value)
    local values = {}
    if value == nil then return values end

    for _,k in ipairs(sorted_keys(value)) do
        v = value[k]
        table.insert(
            values,
            string.format("%s=%s",
                escape_string(k), escape_string(v))
        )
    end

    return values
end

local function tags_fields_tables(config)
    -- FIELDS
    local fields = {}

    -- Get value
    local value = lookup_field_then_value(config.value_field)
    if not value then return nil end
    fields = { value = value }

    -- TAGS
    local tags = {}

    -- Get custom dimensions
    local dimensions_str = lookup_field_then_value(config.dimensions_field)
    if not dimensions_str then return nil end
    local dims = get_dimensions(dimensions_str)
    for k, v in pairs(dims) do
        tags[k] = v
    end

    -- Read default dimensions from message
    local default_dims = get_dimensions(config.default_dimensions)
    for k, v in pairs(default_dims) do
        tags[k] = v
    end

    return encode_fields(fields, config.decimal_precision), encode_tags(tags)
end

local function influxdb_line_msg(config)
    -- reduce timestamp precision if it's not heka default of ns
    local ts
    if config.timestamp_precision and config.timestamp_precision ~= 'ns' then
        ts = field_util.message_timestamp(config.timestamp_precision)
    else
        ts = read_message('Timestamp')
    end

    local series = lookup_field_then_value(config.series_field)
    if not series then return nil end

    local fields, tags = tags_fields_tables(config)
    if not fields or #fields == 0 then return nil end

    if tags and #tags > 0 then
        return string.format("%s,%s %s %d", escape_string(series), table.concat(tags, ","), table.concat(fields, ","), ts)
    else
        return string.format("%s %s %d", escape_string(series), table.concat(fields, ","), ts)
    end
end

local api_messages = {}
function flush()
    if #api_messages > 0 then
        local output = ""
        for _,v in pairs(api_messages) do
            output = output..v.."\n"
        end
        inject_payload("txt", config.payload_name, output)
        api_messages = {}
    end
end

--------------------------------
--
--  Public interface
--
--------------------------------

function process_message()
    api_message = influxdb_line_msg(config, current_batch_count)
    if not api_message then return -1 end

    -- Batch
    table.insert(api_messages, api_message)

    if #api_messages == config.batch_max_count then
        flush()
    end
    return 0
end

function timer_event(ns)
    flush()
end
