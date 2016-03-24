require "os"
require "string"
require "table"

local cjson = require("cjson")

--[=[
Encodes a message to JSON
Config fields are optional.

Config:

- skip_fields (string, optional, default "")
    Space delimited set of fields that should *not* be included in the
    json record being generated. Any fieldname values of "Type",
    "Payload", "Hostname", "Pid", "Logger", "Severity", or "EnvVersion" will
    be assumed to refer to the corresponding field from the base message
    schema, any other values will be assumed to refer to a dynamic message
    field.
- delimiter (string, optional, default "")
    Adds the specified delimiter to the end of each message


*Example Heka Configuration*

.. code-block:: ini
[JSONEncoder]
type = "SandboxEncoder"
script_type = "lua"
filename = "json_encoder.lua"
   [JSONEncoder.config]
    delimiter = "\n"

[HttpOutput]
address = "https://my-json-service.com/"
message_matcher = "<MESSAGE_MATCHER>"
encoder = "JSONEncoder"
--]=]

local skip_fields_str = read_config("skip_fields")
local skip_fields = {}
if skip_fields_str then
    for field in string.gmatch(skip_fields_str, "[%S]+") do
        skip_fields[field] = true
    end
end
local delimiter = read_config("delimiter")
if delimiter == nil then
    delimiter = ""
end
local base_fields = {
    EnvVersion = true,
    Hostname = true,
    Logger = true,
    Payload = true,
    Pid = true,
    Severity = true,
    Timestamp = true,
    Type = true
}


function process_message()
    local output = {}

    -- Process heka fields
    for field in pairs(base_fields) do
        if not skip_fields[field] then
            output[field] = read_message(field)
        end
    end

    -- Process dynamic fields
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end
        if typ ~= 1 then -- exclude bytes
            if not skip_fields[name] then
                output[name] = value
            end
        end
    end

    inject_payload("json", "json", cjson.encode(output) .. delimiter)
    return 0
end
