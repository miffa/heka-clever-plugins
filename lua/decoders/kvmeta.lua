--[=[

Splits a message into multiple messages, with attached routing information.

--]=]

local cjson = require "cjson"
local util = require "util"
local table = require "table"

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

local function copy_message()
    local output = {}

    -- Process heka fields
    for field in pairs(base_fields) do
        output[field] = read_message(field)
    end

    output.Fields = {}

    -- Process dynamic fields
    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        if not base_fields[name] then
            output.Fields[name] = value
        end
    end
    return output
end

local function deepcopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[deepcopy(orig_key)] = deepcopy(orig_value)
        end
        setmetatable(copy, deepcopy(getmetatable(orig)))
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

--------------------------------
--
--  Public interface
--
--------------------------------

function process_message()
    -- Error if _kvmeta isn't set
    local kvmeta = read_message("Fields[_kvmeta]")
    if not kvmeta then return -1 end

    -- Read routes off of message
    local ok, routes = pcall(cjson.decode, kvmeta)
    if not ok or not routes then return -1 end
    MAX_ROUTES = 10
    if #routes > MAX_ROUTES then return -1 end

    -- Copy message completely, then remove routing info
    local msg = copy_message()
    msg.Fields["_kvmeta"] = nil

    -- Inject original message, with routing removed
    inject_message(msg)

    -- Inject one message for each valid route
    for _, route in ipairs(routes) do
        local msg_copy = deepcopy(msg)
        local valid_route = true
        for k, v in pairs(route) do
            -- 'dimensions' must be an array of strings
            if k == "dimensions" then
                if type(v) ~= "table" then
                    valid_route=false
                    break
                end

                if #v == 0 then
                    v = ""
                else
                    v = table.concat(v, " ")
                end
            end

            msg_copy.Fields["_kvmeta." .. k] = v
        end

        if valid_route then inject_message(msg_copy) end
    end

    return 0
end
