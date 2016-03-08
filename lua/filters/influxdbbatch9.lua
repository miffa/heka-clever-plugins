-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[=[
Converts full Heka message contents to line protocol for InfluxDB HTTP write API
(new in InfluxDB v0.9.0).  Optionally includes all standard message fields
as tags or fields and iterates through all of the dynamic fields to add as
points (series), skipping any fields explicitly omitted using the `skip_fields`
config option.  It can also map any Heka message fields as tags in the
request sent to the InfluxDB write API, using the `tag_fields` config option.
All dynamic fields in the Heka message are converted to separate points
separated by newlines that are submitted to InfluxDB.

.. note::
    This encoder is intended for use with InfluxDB versions 0.9 or greater. If
    you're working with InfluxDB versions prior to 0.9, you'll want to use the
    :ref:`config_schema_influx_encoder` instead.

Config:

- decimal_precision (string, optional, default "6")
    String that is used in the string.format function to define the number
    of digits printed after the decimal in number values.  The string formatting
    of numbers is forced to print with floating points because InfluxDB will
    reject values that change from integers to floats and vice-versa.  By
    forcing all numbers to floats, we ensure that InfluxDB will always
    accept our numerical values, regardless of the initial format.

- name_prefix (string, optional, default nil)
    String to use as the `name` key's prefix value in the generated line.
    Supports :ref:`message field interpolation<sandbox_msg_interpolate_module>`.
    `%{fieldname}`. Any `fieldname` values of "Type", "Payload", "Hostname",
    "Pid", "Logger", "Severity", or "EnvVersion" will be extracted from the
    the base message schema, any other values will be assumed to refer to a
    dynamic message field. Only the first value of the first instance of a
    dynamic message field can be used for name name interpolation. If the
    dynamic field doesn't exist, the uninterpolated value will be left in the
    name. Note that it is not possible to interpolate either the
    "Timestamp" or the "Uuid" message fields into the name, those
    values will be interpreted as referring to dynamic message fields.

- name_prefix_delimiter (string, optional, default nil)
    String to use as the delimiter between the name_prefix and the field
    name.  This defaults to a blank string but can be anything else
    instead (such as "." to use Graphite-like naming).

- skip_fields (string, optional, default nil)
    Space delimited set of fields that should *not* be included in the
    InfluxDB measurements being generated. Any `fieldname` values of "Type",
    "Payload", "Hostname", "Pid", "Logger", "Severity", or "EnvVersion" will
    be assumed to refer to the corresponding field from the base message
    schema. Any other values will be assumed to refer to a dynamic message
    field. The magic value "**all_base**" can be used to exclude base fields
    from being mapped to the event altogether (useful if you don't want to
    use tags and embed them in the name_prefix instead).

- source_value_field (string, optional, default nil)
    If the desired behavior of this encoder is to extract one field from the
    Heka message and feed it as a single line to InfluxDB, then use this option
    to define which field to find the value from.  Be careful to set the
    name_prefix field if this option is present or no measurement name
    will be present when trying to send to InfluxDB.  When this option is
    present, no other fields besides this one will be sent to InfluxDB as
    a measurement whatsoever.

- tag_fields (string, optional, default "**all_base**")
    Take fields defined and add them as tags of the measurement(s) sent to
    InfluxDB for the message.  The magic values "**all**" and "**all_base**"
    are used to map all fields (including taggable base fields) to tags and only
    base fields to tags, respectively.  If those magic values aren't used,
    then only those fields defined will map to tags of the measurement sent
    to InfluxDB. The tag_fields values are independent of the skip_fields
    values and have no affect on each other.  You can skip fields from being
    sent to InfluxDB as measurements, but still include them as tags.

- timestamp_precision (string, optional, default "ms")
    Specify the timestamp precision that you want the event sent with.  The
    default is to use milliseconds by dividing the Heka message timestamp
    by 1e6, but this math can be altered by specifying one of the precision
    values supported by the InfluxDB write API (ms, s, m, h). Other precisions
    supported by InfluxDB of n and u are not yet supported.

- value_field_key (string, optional, default "value")
    This defines the name of the InfluxDB measurement.  We default this to "value"
    to match the examples in the InfluxDB documentation, but you can replace
    that with anything else that you prefer.

*Example Heka Configuration*

.. code-block:: ini

    [LoadAvgPoller]
    type = "FilePollingInput"
    ticker_interval = 5
    file_path = "/proc/loadavg"
    decoder = "LinuxStatsDecoder"

    [LoadAvgDecoder]
    type = "SandboxDecoder"
    filename = "lua_decoders/linux_loadavg.lua"

    [LinuxStatsDecoder]
    type = "MultiDecoder"
    subs = ["LoadAvgDecoder", "AddStaticFields"]
    cascade_strategy = "all"
    log_sub_errors = false

    [AddStaticFields]
    type = "ScribbleDecoder"

        [AddStaticFields.message_fields]
        Environment = "dev"

    [InfluxdbLineEncoder]
    type = "SandboxEncoder"
    filename = "lua_encoders/schema_influx_line.lua"

        [InfluxdbLineEncoder.config]
        skip_fields = "**all_base** FilePath NumProcesses Environment TickerInterval"
        tag_fields = "Hostname Environment"
        timestamp_precision= "s"

    [InfluxdbOutput]
    type = "HttpOutput"
    message_matcher = "Type =~ /stats.*/"
    encoder = "InfluxdbLineEncoder"
    address = "http://influxdbserver.example.com:8086/write?db=mydb&rp=mypolicy&precision=s"
    username = "influx_username"
    password = "influx_password"

*Example Output*

.. code-block:: none

    5MinAvg,Hostname=myhost,Environment=dev value=0.110000 1434932024
    1MinAvg,Hostname=myhost,Environment=dev value=0.110000 1434932024
    15MinAvg,Hostname=myhost,Environment=dev value=0.170000 1434932024

--]=]

local decode_message = decode_message
local ipairs = ipairs
local math = require "math"
local read_config = read_config
local read_message = read_message
local pairs = pairs
local string = require "string"
local table = require "table"
local tostring = tostring
local type = type
local os = require "os"

local pattern = "%%{(.-)}"
local _secs

local interp_fields = {
    Type = "Type",
    Hostname = "Hostname",
    Pid = "Pid",
    UUID = "Uuid",
    Logger = "Logger",
    EnvVersion = "EnvVersion",
    Severity = "Severity"
}

------------------------
--
--  msg_interpolate.lua
--
------------------------
local function interpolate_match(match)
    -- First see if it's a primary message schema field.
    local fname = interp_fields[match]
    if fname then
        return read_message(fname)
    end
    -- Second check for a dynamic field.
    fname = string.format("Fields[%s]", match)
    local fval = read_message(fname)
    if type(fval) == "boolean" then
        return tostring(fval)
    elseif fval then
        return fval
    end
    -- Finally try to use it as a strftime format string.
    fval = os.date(match, _secs)
    if fval ~= match then  -- Only return it if a substitution happened.
        return fval
    end
end

function interpolate_from_msg(value, secs)
    _secs = secs
    return string.gsub(value, pattern, interpolate_match)
end

-------------------------------
--
--  End of msg_interpolate.lua
--
-------------------------------

------------------------
--
--  field_util.lua
--
------------------------

base_fields_list = {
    EnvVersion = true,
    Hostname = true,
    Logger = true,
    Payload = true,
    Pid = true,
    Severity = true,
    Type = true
}

base_fields_tag_list = {
    Hostname = true,
    Logger = true,
    Severity = true,
    Type = true
}

local function timestamp_divisor(timestamp_precision)
    -- Default is to divide ns to ms
    local timestamp_divisor = 1e6
    -- Divide ns to s
    if timestamp_precision == "s" then
        timestamp_divisor = 1e9
    -- Divide ns to m
    elseif timestamp_precision == "m" then
        timestamp_divisor = 1e9 * 60
    -- Divide ns to h
    elseif timestamp_precision == "h" then
        timestamp_divisor = 1e9 * 60 * 60
    end
    return timestamp_divisor
end

function field_map(fields_str)
    local fields = {}
    local all_base_fields = false
    local all_fields = false

    if fields_str and fields_str ~= "" then
        for field in string.gmatch(fields_str, "[%S]+") do
            fields[field] = true
            if field == "**all_base**" then
                all_base_fields = true
            end
            if field == "**all**" then
                all_fields = true
            end
        end

        if all_base_fields or all_fields then
            for field in pairs(base_fields_list) do
                fields[field] = true
            end
        end
    else
        fields = base_fields_list
        all_base_fields = true
    end

    return fields, all_base_fields, all_fields
end

function message_timestamp(timestamp_precision)
    local message_timestamp = read_message("Timestamp")
    message_timestamp = math.floor(message_timestamp / timestamp_divisor(timestamp_precision))
    return message_timestamp
end

-- Modified from original to find fields not in skip fields
function used_fields(base_fields, skip_fields)
    local fields = {}
    if skip_fields then
        for field in pairs(base_fields) do
            if not skip_fields[field] then
                fields[field] = true
            end
        end
    end
    return fields
end

local function influxdb_kv_fmt(string)
    return tostring(string):gsub("([ ,])", "\\%1")
end

-- Added utility function to get field values as a table
local function get_field_values(enabled_base_fields)
    local columns = {}
    local values = {}

    columns[1] = "time" -- InfluxDB's default
    values[1] = {name="Timestamp",value={(read_message("Timestamp")/ 1e6)}}

    local place = 2
    for _, field in ipairs(enabled_base_fields) do
        columns[place] = field
        values[place] = {name=field, value={read_message(field)}}
        place = place + 1
    end

    local seen = {}
    local seen_count
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        if name ~= "Timestamp" and typ ~= 1 then -- exclude bytes
            seen_count = seen[name]
            if not seen_count then
                columns[place] = name
                seen[name] = 1
                seen_count = 1
            else
                seen_count = seen_count + 1
                seen[name] = seen_count
                columns[place] = name..tostring(seen_count)
            end
            if count == 1 then
                values[place] = {name=name, value={value}}
            else
                values[place] = {name=name, value={get_array_value(name, seen_count-1, count)}}
            end
            place = place + 1
        end
    end
    return values
end
--------------------------
--
--  End of field_util.lua
--
--------------------------

------------------------
--
--  ts_line_protocol.lua
--
------------------------

-- Modified to handle extra config and change handling of tag fields
-- @mo * removing references to Carbon handling to simplify reasoning
--     * following the line protocol spec at 
--           https://docs.influxdata.com/influxdb/v0.10/write_protocols/line/
--     * adding support for multiple fields in the same line
local function points_tags_tables(config)
    local name_prefix = config.name_prefix or ""
    if config.interp_name_prefix then
        name_prefix = interpolate_from_msg(name_prefix)
    end
    local key_name = config.key_name
    if config.interp_key_name then
        key_name = interpolate_from_msg(key_name)
    end
    local key_field = config.key_field
    local name_prefix_delimiter = config.name_prefix_delimiter or ""
    local used_tag_fields = config.used_tag_fields
    local skip_fields = config.skip_fields

    -- Initialize the tags table, including base field tag values in
    -- list if the magic **all** or **all_base** config values are
    -- defined.
    local tags = {}
    local testoutput = ""
    if (config.tag_fields_all
        or config.tag_fields_all_base
        or config.used_tag_fields) then
            for field in pairs(used_fields(base_fields_tag_list, config.skip_fields)) do
                if config.tag_fields_all or config.tag_fields_all_base or used_tag_fields[field] then
                    local value = read_message(field)
                    table.insert(tags, influxdb_kv_fmt(field).."="..tostring(influxdb_kv_fmt(value)))
                end
            end
    end

    -- Initialize the table of data points and populate it with data
    -- from the Heka message.  When skip_fields includes "**all_base**",
    -- only dynamic fields are included as InfluxDB data points, while
    -- the base fields serve as tags for them. If skip_fields does not
    -- define any base fields, they are added to the fields of each data
    -- point and each dynamic field value is set as the "value" field.
    -- Setting skip_fields to "**all_base**" is recommended to avoid
    -- redundant data being stored in each data point (the base fields
    -- as fields and as tags).
    local points = {}

    local msg_fields = get_field_values(used_fields(base_fields_list, config.skip_fields))

    if msg_fields then
        for _, field_entry in ipairs(msg_fields) do
            local field = field_entry["name"]
            local value
            for _, field_value in ipairs(field_entry["value"]) do
                value = field_value
            end

            -- Include the dynamic fields as tags if they are defined in
            -- configuration or the magic value "**all**" is defined.
            -- Convert value to a string as this is required by the API
            if not config["carbon_format"]
                and (config["tag_fields_all"]
                or (config["used_tag_fields"] and used_tag_fields[field])) then
                    table.insert(tags, influxdb_kv_fmt(field).."="..tostring(influxdb_kv_fmt(value)))
            end

            if key_name then
                if key_field then
                    if field == key_field then
                        points[key_name] = value
                    end
                else
                    points[key_name] = value
                end
            elseif config["source_value_field"] then 
                if field == config["source_value_field"] then
                    points[name_prefix] = value
                end
                -- Only add fields that are not requested to be skipped
            elseif not config["skip_fields_str"]
                or (config["skip_fields"] and not skip_fields[field]) then
                    -- Set the name attribute of this table by concatenating name_prefix
                    -- with the name of this particular field
                    points[field] = value
            end
        end
    else
        return 0
    end

    return points, tags
end

function influxdb_line_msg(config)
    local name_prefix = config.name_prefix or ""
    if config.interp_name_prefix then
        name_prefix = interpolate_from_msg(name_prefix)
    end
    local api_message = ""
    local message_timestamp = message_timestamp(config.timestamp_precision)
    local points, tags = points_tags_tables(config)
    local fields = {}

    -- Build a table of data points that we will eventually convert
    -- to a newline delimited list of InfluxDB write API line protocol
    -- formatted values that are then injected back into the pipeline.
    for name, value in pairs(points) do
        -- Wrap in double quotes and escape embedded double quotes
        -- as defined by the protocol.
        if type(value) == "string" then
            value = '"'..value:gsub('"', '\\"')..'"'
        end

        -- Always send numbers as formatted floats, so InfluxDB will accept
        -- them if they happen to change from ints to floats between
        -- points in time.  Forcing them to always be floats avoids this.
        -- Use the decimal_precision config option to control the
        -- numbers after the decimal that are printed.
        if type(value) == "number" or string.match(value, "^[%d.]+$") then
            value = string.format("%."..config.decimal_precision.."f", value)
        end
        
        -- add this field  to the list of fields
        table.insert(fields, influxdb_kv_fmt(name).."="..tostring(influxdb_kv_fmt(value)))
    end

    -- @mo Format the line differently based on the presence of tags and fields 
    -- both fields and tags are present
    if tags and #tags > 0 and fields and #fields > 0 then
        api_message = string.format("%s,%s %s %d", influxdb_kv_fmt(name_prefix), table.concat(tags, ","),
                                     table.concat(fields, ","), message_timestamp)
    -- only fields, no tags
    elseif fields and #fields > 0 then
        api_message = string.format("%s %s %d", influxdb_kv_fmt(name_prefix), table.concat(fields, ","),
                                     message_timestamp)
    end

    return api_message
end

function set_config(client_config)
    -- Initialize table with default values for ts_line_protocol module
    local module_config = {
        carbon_format = false,
        decimal_precision = "6",
        key_field = false,
        key_name = false,
        name_prefix = false,
        name_prefix_delimiter = false,
        skip_fields_str = false,
        source_value_field = false,
        tag_fields_str = "**all_base**",
        timestamp_precision = "ms",
        value_field_key = "value"
    }

    -- Update module_config defaults with those found in client configs
    for option in pairs(module_config) do
        if client_config[option] then
            module_config[option] = client_config[option]
        end
    end

    -- Remove blacklisted fields from the set of base fields that we use, and
    -- create a table of dynamic fields to skip.
    if module_config.skip_fields_str then
        module_config.skip_fields,
        module_config.skip_fields_all_base = field_map(client_config.skip_fields_str)
        module_config.used_base_fields = used_fields(base_fields_list, module_config.skip_fields)
    end

    -- Create and populate a table of fields to be used as tags
    if module_config.tag_fields_str then
        module_config.used_tag_fields,
        module_config.tag_fields_all_base,
        module_config.tag_fields_all = field_map(client_config.tag_fields_str)
    end

    -- Cache whether or not name_prefix needs interpolation
    module_config.interp_name_prefix = false
    if module_config.name_prefix and string.find(module_config.name_prefix, "%%{[%w%p]-}") then
        module_config.interp_name_prefix = true
    end

    -- Cache whether or not key_name needs interpolation
    module_config.interp_key_name = false
    if module_config.key_name and string.find(module_config.key_name, "%%{[%w%p]-}") then
        module_config.interp_key_name = true
    end

    return module_config
end

local decoder_config = {
    decimal_precision = read_config("decimal_precision") or "6",
    key_field = read_config("key_field") or nil,
    key_name = read_config("key_name") or nil,
    name_prefix = read_config("name_prefix") or nil,
    name_prefix_delimiter = read_config("name_prefix_delimiter") or nil,
    skip_fields_str = read_config("skip_fields") or nil,
    source_value_field = read_config("source_value_field") or nil,
    tag_fields_str = read_config("tag_fields") or "**all_base**",
    timestamp_precision = read_config("timestamp_precision") or "ms",
    value_field_key = read_config("value_field_key") or "value"
}

--------------------------------
--
--  End of ts_line_protocol.lua
--
--------------------------------

local config = set_config(decoder_config)

api_messages = {}
batch_max_count = read_config("max_count") or 20

--
-- Public Interface 
--

function process_message()
    -- Inject a new message with the payload populated with the newline
    -- delimited data points, and append a newline at the end for the last line
    api_message = influxdb_line_msg(config)

    table.insert(api_messages, api_message)
    if #api_messages == batch_max_count then
       local output = ""
       for k,v in pairs(api_messages) do
          output = output..v.."\n"
       end
       inject_payload("txt", "influxdbbatch9", output)
       api_messages = {}
    end

    return 0
end

function timer_event(ns)
   -- details of the lua sandbox guarantee that this timer
   -- does not get called in the middle of a process_message call
   if #api_messages > 0 then
      local output = ""
      for k,v in pairs(api_messages) do
        output = output..v.."\n"
      end
      inject_payload("txt", "influxdbbatch9", output)
      api_messages = {}
   end
end

--
-- END Public Interface
--
