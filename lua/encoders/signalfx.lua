-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[=[

Extracts data from message fields in messages.
Generates JSON suitable for send to datapoint SignalFx API.

(Currently only supports writing data points, NOT events.)

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

- stat_type (string, optional, defaults to "counter")
    A metric can be of type "counter" or "gauge".

*Example Heka Configuration*

.. code-block:: ini

    [signalfx-encoder]
    type = "SandboxEncoder"
    filename = "lua_encoders/signalfx.lua"
       [signalfx-encoder.config]
       metric_name = "test-metric.%{title}.%{Hostname}"
       value_field = "metric_value"
       stat_type = "gauge"

    [signalfx]
    type = "HttpOutput"
    message_matcher = "Type == 'json'"
    address = "https://ingest.signalfx.com/v2/datapoint"
    encoder = "signalfx-encoder"
      [signalfx.headers]
      content-type = ["application/json"]
      X-SF-Token = "<YOUR-SIGNALFX-API-TOKEN>"

--]=]


require "cjson"
require "string"
require "table"

local metric_name = read_config("metric_name")
local value_field = read_config("value_field") or "value"

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

-- Used for interpolating message fields into series name.
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

function process_message()
    local ts = read_message("Timestamp") / 1e9
    if not ts then return -1 end

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

    local output = {
        -- array of data points
        [stat_type] = {
            {
                metric=name,
                value=value,
                timestamp=ts,
                dimensions={
                    hostname=Hostname
                }
            }
        }
    }

    inject_payload("json", "signalfx", cjson.encode(output))
    return 0
end
