-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Parses a payload containing JSON. Does not modify any Heka message 
attributes, only adds to the `Fields`.

Config:

- type (string, optional, default json):
    Sets the message 'Type' header to the specified value
- strict (boolean, optional, default true):
    Should we assume that the entire payload is json parsable. When set to 
    `false` non-json string in the payload are added to `_prefix` and `_postfix`
    fields respectively.
- keep_payload (boolean, optional, default false):
    Always preserve the original log line in the message payload.

*Example Heka Configuration*

.. code-block:: ini

    [LogInput]
    type = "LogstreamerInput"
    log_directory = "/var/log"
    file_match = 'application\.json'
    decoder = "JsonDecoder"

    [JsonDecoder]
    type = "SandboxDecoder"
    filename = "lua_decoders/json_decoder.lua"

        [JsonDecoder.config]
        type = "json"
        strict = true
        payload_keep = True
--]]

require "cjson"
require "string"

local msg_type = read_config("type") or 'json'
local strict_parsing = read_config("strict")
if strict_parsing == nil or strict_parsing == '' then
    strict_parsing = true
end
local keep_payload = read_config("payload_keep")
if keep_payload == nil or keep_payload == '' then
    keep_payload = false
end


function process_message()
    local payload = read_message("Payload")

    -- check length (cjson.encode will crash if payload is > 11500 characters)
    if #payload > 11500 then
       return -1
    end

    local ok, json = pcall(cjson.decode, payload)
    local prefix = ''
    local postfix = ''

    if strict_parsing and not ok then
        return -1 
    elseif not ok then
        -- try parsing once more by stripping extra content on beg and end
        local json_start = string.find(payload, "{")
        local json_end = string.match(payload, '^.*()}')
        if json_start == nil or json_end == nil then
            return -1
        end

        prefix = string.sub(payload, 0, json_start-1)
        postfix = string.sub(payload, json_end+1)
        ok, json = pcall(cjson.decode, string.sub(payload, json_start, json_end))

    end

    if not ok then return -1 end
    if type(json) ~= "table" then return -1 end

    if keep_payload then
        write_message("Payload", cjson.encode(json))
    else
        -- Clear the payload now that it is all parsed
        write_message("Payload", "")
    end
    write_message("Fields[_prefix]", prefix)
    write_message("Fields[_postfix]", postfix)

    for k, v in pairs(json) do
        -- nested json strings are not supported, stringify them
        if type(v) == "table" then
            write_message("Fields[" .. k .. "]", cjson.encode(v))
        else
            write_message("Fields[" .. k .. "]", v)
        end
    end

    write_message("Type", msg_type)
    return 0
end
