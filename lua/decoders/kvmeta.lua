--[=[

Splits a message into multiple messages, with attached routing information.

Config:

- msg_type (string, optional):
    Sets the message 'Type' field. If unset, has no effect on the message's 'Type'.

--]=]

local cjson = require "cjson"
local field_util = require "field_util"
local table = require "table"

local config = {
    msg_type = read_config("msg_type")
}

local base_fields = field_util.field_map()
base_fields['Timestamp'] = true

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
    local copy
    if type(orig) == 'table' then
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
    local kvmeta_field = read_message("Fields[_kvmeta]")
    if not kvmeta_field then return -1 end

    local ok, kvmeta = pcall(cjson.decode, kvmeta_field)
    if not ok or not kvmeta then return -1 end

    local routes = kvmeta["routes"]
    if not routes then return -1 end
    MAX_ROUTES = 10
    if #routes > MAX_ROUTES then return -1 end

    -- For backwards compatilibity, ignore _kvmeta and set `Kayvee` type if no routes were emitted
    if #routes == 0 then
        write_message("Type", "Kayvee")
        write_message("Fields[_kvmeta]", nil)
        return 0
    end

    -- Copy the message, so we can modify inject various routed versions of it.
    --  * `_kvmeta` routing info
    --  * set msg.Type, if it was specified in the Decoder's config
    local msg = copy_message()
    msg.Fields["_kvmeta"] = nil
    for k, v in pairs(kvmeta) do
        -- Append metadata for everything except "routes".
        -- We handle each route below.
        -- This adds things like `kv_version` or `kv_language`.
        if k ~= "routes" then
            msg.Fields["_kvmeta."..k] = v
        end
    end

    if config.msg_type then msg.Type = config.msg_type end

    -- Inject original message, with type 'logs'
    local msg_copy = deepcopy(msg)
    msg_copy.Fields["_kvmeta.type"] = 'logs'

    rules = nil
    for _, route in ipairs(routes) do
        if rules then
            rules = rules .. " " .. route["rule"]
        else
            rules = route["rule"]
        end
    end
    msg_copy.Fields["_kvmeta.route-rules"] = rules

    inject_message(msg_copy)

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
