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

- ticker_interval (int, optional, defaults to never sending a ticker event)
    Max time delay before a batch is flushed from the filter.

- timestamp_precision (string, optional, default "ms")
    Specify the timestamp precision that you want the event sent with.  The
    default is to use milliseconds by dividing the Heka message timestamp
    by 1e6, but this math can be altered by specifying one of the precision
    values supported by the InfluxDB write API (ms, s, m, h). Other precisions
    supported by InfluxDB of n and u are not yet supported.

*Example Heka Configuration*
- value_field (string, required)
    The `fieldname` to use as the value for the metric in signalfx. If the `value`
    field is not present this encoder will set one as the value for counters: `1`.
    A value of `0` will be used for `gauges`.


*Example Heka Configuration*

.. code-block:: ini

    [KayveeInfluxdblineBatchFilter]
    message_matcher = "TRUE"
    type = "SandboxFilter"
    script_type = "lua"
    filename = "lua/filters/kayvee_signalfxbatch_messsage.lua"

        [KayveeInfluxdblineBatchFilter.config]
        series_field="series_f"
        value_field="value_f"
        stat_type_field="stat_type_f"
        dimensions_field="dimensions_f"
        default_dimensions="Hostname"
        max_count = 1000
        ticker_interval = 60

    [InfluxdbOutput]
    type = "HttpOutput"
    message_matcher = "Fields[payload_name] == 'influxdblinebatch'"
    encoder = "PayloadEncoder"
    address = "http://influxdbserver.example.com:8086/write?db=mydb&rp=mypolicy&precision=s"
    username = ["%ENV[INFLUXDB_USERNAME]"]
    password = ["%ENV[INFLUXDB_PASSWORD]"]

--]=]

local interp = require "msg_interpolate"
local field_util = require "field_util"

local string = require "string"
local table = require "table"

local decode_message = decode_message
local read_config = read_config
local read_message = read_message

local ipairs = ipairs
local pairs = pairs
local tostring = tostring
local type = type

------------------------
--
--  Private Interface
--
------------------------

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

function escape_string(str)
    return tostring(str):gsub("([ ,])", "\\%1")
end

function encode_scalar_value(value, decimal_precision)
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

function sorted_keys(map)
	sorted = {}
    for key in pairs(map) do table.insert(sorted, key) end
    table.sort(sorted)
	return sorted
end

function encode_fields(fields, decimal_precision)
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

function encode_tags(value)
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

function influxdb_line_msg(config)
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

    -- TODO: @n Do full alpha sorting instead of (A-Za-z). case sensitive currently.
    --      depends on locale... http://lua-users.org/lists/lua-l/2009-12/msg00658.html
    if tags and #tags > 0 and fields and #fields > 0 then
        return string.format("%s,%s %s %d", escape_string(series), table.concat(tags, ","),
                                     table.concat(fields, ","), ts)
    -- only fields, no tags. at least one field is required by the line protocol
    elseif fields and #fields > 0 then
        return string.format("%s %s %d", escape_string(series), table.concat(fields, ","),
                                     ts)
    else
		return nil
    end
end

--------------------------------
--
--  End of private functions
--
--------------------------------
local config
function configure()
    config = {
		series_field = read_config("series_field") or error("series_field must be specified"),
        value_field = read_config("value_field") or error("value_field must be specified"),
        dimensions_field = read_config("dimensions_field") or error("dimensions_field must be specified") ,
        default_dimensions = read_config("default_dimensions") or error("default_dimensions must be specified") ,

        decimal_precision = read_config("decimal_precision") or "6",
        timestamp_precision = read_config("timestamp_precision") or "ms",
        payload_name = read_config("payload_name") or "influxdblinebatch",
    }
end
configure()

api_messages = {}
batch_max_count = read_config("max_count") or 20

--
-- Public Interface
--

function process_message()
    api_message = influxdb_line_msg(config, current_batch_count)
	if not api_message then return -1 end

    -- Batch
    table.insert(api_messages, api_message)

    if #api_messages == batch_max_count then
        flush()
    end
    return 0
end

function timer_event(ns)
    flush()
end

function flush()
    if #api_messages > 0 then
        local output = ""
        for k,v in pairs(api_messages) do
	        output = output..v.."\n"
        end
        inject_payload("txt", config.payload_name, output)
        api_messages = {}
    end
end

--
-- END Public Interface
--
