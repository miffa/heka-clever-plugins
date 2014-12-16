-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Parses a payload containing JSON. Does not modify any Heka message 
attributes, only adds to the `Fields`.

Config:

- type (string, optional, default json):
    Sets the message 'Type' header to the specified value

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
--]]

require "cjson"

local msg_type = read_config("type") or 'json'

function process_message()
    local ok, json = pcall(cjson.decode, read_message("Payload"))
    if not ok then
        return -1
    end
    if type(json) ~= "table" then return -1 end
    
    for k, v in pairs(json) do
        write_message("Fields[" .. k .. "]", v)
    end
    write_message("Type", msg_type)

    return 0
end
