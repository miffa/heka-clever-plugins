require 'os'
local util = require 'util'

--------------------
-- Private
--------------------
local module = {}

local DEFAULT_MOCKS = {
    cfg = {},
    injected_payloads = {},
    injected_messages = {},
    next_message=nil,
    next_field=1,
    fields = {},
    written_messages = {},
    read_next_field = {
        calls = 0,
        fields = {},
    }
}
local MOCKS = util.deepcopy(DEFAULT_MOCKS)

-- set env var `DEBUG=1` to print debug logs
DEBUG = tonumber(os.getenv("DEBUG"))

local function debug(s)
    if DEBUG then print("[DEBUG] " .. s) end
end

--------------------
-- Public
--------------------

-- sets the config object, used by `read_config`
function module.set_config(cfg)
    debug("set_config")
    MOCKS.cfg = cfg
end

-- sets next message for `process_message` to read
function module.set_next_message(msg)
    debug("set_next_message")
    MOCKS.next_message = msg

    MOCKS.next_field = 1
    MOCKS.fields = {}
    for k, v in pairs(msg) do
        if k:find("Fields%[") ~= nil then
            MOCKS.fields[#MOCKS.fields+1] = {
                name=k:sub(8, -2), -- sub to trim off "Fields[" and "]"
                value=v
            }
        end
    end
    MOCKS.read_next_field_calls = 0
end

-- return all injected payloads
function module.injected_payloads()
    debug("injected_payloads")
    for i, v in ipairs(MOCKS.injected_payloads) do
        debug(i .. "\t" .. tostring(v.data))
    end
    return MOCKS.injected_payloads
end

-- return all written messages
function module.written_messages()
    debug("written_messages")
    return MOCKS.written_messages
end

-- return all injected messages
function module.injected_messages()
    debug("injected_messages")
    for i, v in ipairs(MOCKS.injected_messages) do
        debug(i .. "\t" .. tostring(v.data))
    end
    return MOCKS.injected_messages
end

-- reset mocks to default state
function module.reset()
    MOCKS = util.deepcopy(DEFAULT_MOCKS)
end

---------------------------------------
-- Mocks for the Heka Lua Sandbox
-- http://hekad.readthedocs.io/en/v0.10.0/sandbox/
--
-- These fuctions are injected into global scope. They aren't intended to be
-- called directly in the tests; instead, they are invoked by the plugin itself.
--
-- The test should call the plugin's `process_message` and `ticker_event` functions
---------------------------------------

function read_config(key)
    debug("heka.read_config: " .. key)
    return MOCKS.cfg[key]
end

function read_message(field)
    debug("heka.read_message: " .. field)
    if not MOCKS.next_message then
        assert(False, "No next_message set in Heka mocks")
    end

    if field == "raw" then
        -- 'raw' is a special value. if read_message('raw') is called, it returns the underlying message struct
        local raw_msg = {
            Fields={}
        }
        for k, v in pairs(MOCKS.next_message) do
            if not string.match(k, "Fields") then
                local to_insert = {
                    name=k,
                    value={v},
                }
                table.insert(raw_msg.Fields, to_insert)
            end
        end
        return raw_msg
    end

	-- Return non-base field
	if string.match(field, 'Fields%[') then
		field = string.sub(field, 8, -2)
		return MOCKS.next_message.Fields[field]
	end

	-- Return Heka base field
    return MOCKS.next_message[field]
end

function read_next_field()
    debug("heka.read_next_field")

    if not MOCKS.next_message then
        assert(False, "No next_message set in Heka mocks")
    end

    if MOCKS.next_field > #MOCKS.fields then
        return false, nil, nil, nil, #MOCKS.fields
    end

    local field = MOCKS.fields[MOCKS.next_field]
    MOCKS.next_field = MOCKS.next_field + 1

    -- Returns type (which assumed also be a string [type 0]), name, value, representation, count
    return 0, field["name"], field["value"], "-", #MOCKS.fields
end

function write_message(name, value)
    debug("heka.write_message: "..name)

    if value == nil then
        value = "__REMOVED_FIELD__"
    end

    MOCKS.written_messages[name] = value
end

function inject_payload(payload_type, payload_name, data)
    debug("heka.inject_payload: " .. tostring(payload_type) .. " " .. tostring(payload_name) .. " " .. tostring(data))
    table.insert(MOCKS.injected_payloads, {
        payload_type = payload_type,
        payload_name = payload_name,
        data = data,
    })
end

function read_next_field()
    MOCKS.read_next_field.calls = MOCKS.read_next_field.calls + 1
    debug("heka.read_next_field:" .. MOCKS.read_next_field.calls)
    -- On the field call to read_next_field, save the fields.
    -- This ensures they are read in a consistent order.
    if MOCKS.read_next_field.calls == 1 then
        for k, v in pairs(MOCKS.next_message) do
            table.insert(MOCKS.read_next_field.fields, {
                name=k,
                value=v,
            })
        end
    end

    -- Once we've read all the fields, return nil
    idx = MOCKS.read_next_field.calls
    if idx > #MOCKS.read_next_field.fields then return nil end

    -- Return the field
    typ = "type"
    name = MOCKS.read_next_field.fields[idx].name
    value = MOCKS.read_next_field.fields[idx].value
    repr = "representation"
    count = 1
    return typ, name, value, repr, count
end

function inject_message(message)
    debug("heka.inject_message: " .. tostring(message))
    table.insert(MOCKS.injected_messages, message)
end

function decode_message(s)
    debug("heka.decode_message: " .. tostring(s))
    -- decode_message(heka_protobuf_string)
    --  * actual behavior: Converts a Heka protobuf encoded message string into a Lua table.
    --  * mocked behavior: No-op. Return the string without modification.
    return s
end

return module
