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
--]]

require "cjson"
require "string"

local msg_type = read_config("type") or 'json'
local strict_parsing = read_config("strict")
if strict_parsing == nil or strict_parsing == '' then
    strict_parsing = true
end

function process_message()
    local payload = read_message("Payload")
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

    write_message("Fields[_prefix]", prefix)
    write_message("Fields[_postfix]", postfix)

    for k, v in pairs(json) do
        write_message("Fields[" .. k .. "]", v)
    end
    write_message("Type", msg_type)

    return 0
end
