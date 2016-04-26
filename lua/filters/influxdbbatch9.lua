-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[=[
Converts full Heka message contents to line protocol for InfluxDB HTTP write API
(new in InfluxDB v0.9.0). This filter converts each Heka message into one InfluxDB
line and seperates multiple lines with `\n`. The `name` is re-evaluated for each line.

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

- name (string, required)
    String to use as the metric `name` in the generated line.
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

- skip_fields (string, optional, default nil)
    Space delimited set of fields that should *not* be included as `fields` for 
    InfluxDB measurements being generated. Any `fieldname` values of "Type",
    "Payload", "Hostname", "Pid", "Logger", "Severity", or "EnvVersion" will
    be assumed to refer to the corresponding field from the base message
    schema. Any other values will be assumed to refer to a dynamic message
    field. The magic value "**all_base**" can be used to exclude base fields
    from being mapped to the event altogether. Does not interact with `tag_fields`.

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
--  some functions borrwoed from ts_line_protocol.lua
--
------------------------

local function name_prefill(config)
    local name = config.name or ""
    if config.interp_name then
        name = interp.interpolate_from_msg(name)
    end
    return name
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
        -- first unescape already escaped `"`
        value_dec = value:gsub('\\"', '"')
        -- string values need to be double quoted
        return '"' .. value_dec:gsub('"', '\\"') .. '"'
    elseif type(value) == "boolean" then
        -- don't quote booleans
        return tostring(value)
    end
end

function encode_fields(value, decimal_precision)
    local values = {}
    if type(value) == "table" then
        for k,v in pairs(value) do
            table.insert(
                values,
                string.format("%s=%s", escape_string(k), encode_scalar_value(v, decimal_precision))
            )
        end
    else
        values["value"] = encode_scalar_value(value, decimal_precision)
    end
    return values
end

function encode_tags(value)
    local values = {}
    for k,v in pairs(value) do
        table.insert(
            values,
            string.format("%s=%s",
                escape_string(k), escape_string(v))
        )
    end

    return values
end


-- Modified to handle extra config and change handling of tag fields
-- @mo * removing references to Carbon handling to simplify reasoning
--     * following the line protocol spec at 
--           https://docs.influxdata.com/influxdb/v0.10/write_protocols/line/
--     * adding support for multiple fields in the same line
local function tags_fields_tables(config)
    -- Initialize the tags table, including base field tag values in
    -- list if the magic **all** or **all_base** config values are
    -- defined.
    local tags = {}
    if (config.tag_fields_all or config.tag_fields_all_base or config.used_tag_fields) then
        for field in pairs(field_util.used_base_fields(config.skip_fields)) do
            if config.tag_fields_all or config.tag_fields_all_base or used_tag_fields[field] then
                tags[field] = read_message(field)
            end
        end
    end

    -- Initialize the table of data points and populate it with data
    -- from the Heka message.  When skip_fields includes "**all_base**",
    -- only dynamic heka fields are included as InfluxDB data fields, while
    -- the base fields serve as tags for them. If skip_fields does not
    -- define any base fields, they are added to the fields of each data
    -- point and each dynamic field value is set as fields.
    local fields = {}
    local msg = decode_message(read_message("raw"))

    if msg.Fields then
        for _, field_entry in ipairs(msg.Fields) do
            local field = field_entry["name"]
            local value
            for _, field_value in ipairs(field_entry["value"]) do
                value = field_value
            end

            -- Include the dynamic fields as tags if they are defined in
            -- configuration or the magic value "**all**" is defined.
            -- Convert value to a string as this is required by the API
            if (config["tag_fields_all"]
                or (config["used_tag_fields"] and used_tag_fields[field])) then
                tags[field] = value
            end

            if not config["skip_fields_str"]
                or (config["skip_fields"] and not skip_fields[field]) then
                    fields[field] = value
            end
        end
    else
        return 0
    end

    return encode_fields(fields, config.decimal_precision), encode_tags(tags)
end

function influxdb_line_msg(config)
    -- reduce timestamp precision if it's not heka default of ns
    local ts
    if config.time_precision  and config.time_precision ~= 'ns' then
        ts = field_util.message_timestamp(config.time_precision)
    else
        ts = read_message('Timestamp')
    end

    local name = name_prefill(config)
    local fields, tags = tags_fields_tables(config)

    -- @mo Format the line differently based on the presence of tags and fields 
    -- both fields and tags are present
    local api_message = ""
    -- TODO: @mo sort tags since influx like that for performance
    if tags and #tags > 0 and fields and #fields > 0 then
        api_message = string.format("%s,%s %s %d", escape_string(name), table.concat(tags, ","),
                                     table.concat(fields, ","), ts)
    -- only fields, no tags. at least one field is required by the line protocol
    elseif fields and #fields > 0 then
        api_message = string.format("%s %s %d", escape_string(name), table.concat(fields, ","),
                                     ts)
    end

    return api_message
end

function set_config(client_config)
    local module_config = client_config

    -- Remove blacklisted fields from the set of base fields that we use, and
    -- create a table of dynamic fields to skip.
    if module_config.skip_fields_str then
        module_config.skip_fields,
        module_config.skip_fields_all_base = field_util.field_map(client_config.skip_fields_str)
        module_config.used_base_fields = field_util.used_base_fields(module_config.skip_fields)
    end

    -- Create and populate a table of fields to be used as tags
    if module_config.tag_fields_str then
        module_config.used_tag_fields,
        module_config.tag_fields_all_base,
        module_config.tag_fields_all = field_map(client_config.tag_fields_str)
    end

    -- Cache whether or not name needs interpolation
    module_config.interp_name = false
    if module_config.name and string.find(module_config.name, "%%{[%w%p]-}") then
        module_config.interp_name = true
    end

    return module_config
end

local filter_config = {
    name = read_config("name") or nil,
    decimal_precision = read_config("decimal_precision") or "6",
    skip_fields_str = read_config("skip_fields") or nil,
    tag_fields_str = read_config("tag_fields") or "**all_base**",
    timestamp_precision = read_config("timestamp_precision") or "ms",
    payload_name = read_config("payload_name") or "influxdblinebatch",
}

--------------------------------
--
--  End of private functions
--
--------------------------------

local config = set_config(filter_config)

api_messages = {}
batch_max_count = read_config("max_count") or 20

--
-- Public Interface 
--

function process_message()
    -- Inject a new message with the payload populated with the newline
    -- delimited data points, and append a newline at the end for the last line
    api_message = influxdb_line_msg(config, current_batch_count)

    table.insert(api_messages, api_message)
    if #api_messages == batch_max_count then
       local output = ""
       for _,v in pairs(api_messages) do
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
