
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[=[
Converts full Heka message contents to JSON for InfluxDB HTTP API, batches all
messages seen in ticker_interval, and outputs a new heka message.  The new message
has Fields[payload_name] equal to 'influxdbbatch', unless overrideen in config.

Each JSON object includes all standard message fields and all of the dynamically
specified fields, skipping any bytes fields or any fields explicitly omitted
using the `skip_fields` config option.

Config:

- series (string, optional, default "series")
    String to use as the `series` key's value in the generated JSON. Supports
    interpolation of field values from the processed message, using
    `%{fieldname}`. Any `fieldname` values of "Type", "Payload", "Hostname",
    "Pid", "Logger", "Severity", or "EnvVersion" will be extracted from the
    the base message schema, any other values will be assumed to refer to a
    dynamic message field. Only the first value of the first instance of a
    dynamic message field can be used for series name interpolation. If the
    dynamic field doesn't exist, the uninterpolated value will be left in the
    series name. Note that it is not possible to interpolate either the
    "Timestamp" or the "Uuid" message fields into the series name, those
    values will be interpreted as referring to dynamic message fields.

- skip_fields (string, optional, default "")
    Space delimited set of fields that should *not* be included in the
    InfluxDB records being generated. Any fieldname values of "Type",
    "Payload", "Hostname", "Pid", "Logger", "Severity", or "EnvVersion" will
    be assumed to refer to the corresponding field from the base message
    schema, any other values will be assumed to refer to a dynamic message
    field.

- payload_name (string, optional, default "influxdbbatch")
    Sets the value of Fields[payload_name] on each message.


*Example Heka Configuration*

.. code-block:: ini

    [influxdbbatch]
    type = "SandboxFilter"
    message_matcher = "Type == 'influxdb'
    ticker_interval = 5
    filename = "/heka/clever/filters/influxdbbatch.lua"
        [influxdbbatch.config]
        series = "heka.%{Logger}"
        skip_fields = "Pid EnvVersion"

    [InfluxOutput]
    message_matcher = "Fields[payload_name] == 'influxdbbatch'"
    encoder = "influxdb"
    type = "HttpOutput"
    address = "http://influxdbserver.example.com:8086/db/databasename/series"
    username = "influx_username"
    password = "influx_password"

*Example Output*

.. code-block:: json

    [{"points":[[1.409378221e+21,"log","test","systemName","TcpInput",5,1,"test"]],"name":"heka.MyLogger","columns":["Time","Type","Payload","Hostname","Logger","Severity","syslogfacility","programname"]}]

--]=]

require "cjson"
require "string"
require "table"

local payload_name = read_config("payload_name") or "influxdbbatch"
local series_orig  = read_config("series") or "series"
local series = series_orig
local use_subs
if string.find(series, "%%{[%w%p]-}") then
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

local base_fields_list = {
    "Type",
    "Payload",
    "Hostname",
    "Pid",
    "Logger",
    "Severity",
    "EnvVersion"
}

-- Used for interpolating message fields into series name.
local function sub_func(key)
    if base_fields_map[key] then
        return read_message(key)
    else
        local val = read_message("Fields["..key.."]")
        if val then
            return val
        end
        return "%{"..key.."}"
    end
end

-- Remove blacklisted fields from the set of base fields that we use, and
-- create a table of dynamic fields to skip.
local used_base_fields = {}
local skip_fields_str = read_config("skip_fields")
local skip_fields = {}
if skip_fields_str then
    for field in string.gmatch(skip_fields_str, "[%S]+") do
        skip_fields[field] = true
    end
    for _, base_field in ipairs(base_fields_list) do
        if not skip_fields[base_field] then
            used_base_fields[#used_base_fields+1] = base_field
        else
            skip_fields[base_field] = nil
        end
    end
else
    used_base_fields = base_fields_list
end

local function get_array_value(field, field_idx, count)
    local value = {}
    for i = 1,count do
        value[i] = read_message("Fields["..field.."]",field_idx,i-1)
    end
    return value
end

encoded_messages = {}
batch_max_count = read_config("max_count") or 20

function process_message()
    local columns = {}
    local values = {}

    columns[1] = "time" -- InfluxDB's default
    values[1] = read_message("Timestamp") / 1e6

    local place = 2
    for _, field in ipairs(used_base_fields) do
        columns[place] = field
        values[place] = read_message(field)
        place = place + 1
    end

    local seen = {}
    local seen_count
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        if name ~= "Timestamp" and typ ~= 1 then -- exclude bytes
            if not skip_fields_str or not skip_fields[name] then
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
                    values[place] = value
                else
                    values[place] = get_array_value(name, seen_count-1, count)
                end
                place = place + 1
            end
        end
    end

    if use_subs then
        series = string.gsub(series_orig, "%%{([%w%p]-)}", sub_func)
    end

    local output = {
       name = series,
       columns = columns,
       points =  {values}
    }
    table.insert(encoded_messages, output)
    if #encoded_messages == batch_max_count then
       inject_payload("json", payload_name, cjson.encode(encoded_messages))
       encoded_messages = {}
    end
    return 0
end

function timer_event(ns)
   -- details of the lua sandbox guarantee that this timer
   -- does not get called in the middle of a process_message call
   if #encoded_messages > 0 then
      inject_payload("json", payload_name, cjson.encode(encoded_messages))
      encoded_messages = {}
   end
end
