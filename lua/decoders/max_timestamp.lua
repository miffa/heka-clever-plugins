--[=[

Sets Type = `MaxTimestamp` for any message a timestamp >= max_timestamp.

max_timestamp is a configuration parameter, and expected a unix timestamp in seconds.

--]=]

local math = require 'math'
local os = require 'os'

-- Read and validate config for `max_timestamp`
local max_timestamp = read_config("max_timestamp") or error("max_timestamp must be set in config")
max_timestamp = tonumber(max_timestamp) or error("max_timestamp must be a number")
if not (max_timestamp > -2) then error("max_timestamp must be >= 0 (a unix timestamp) or -1 (makes max_timestamp a no-op decoder)") end

local config = {
    max_timestamp = max_timestamp
}

--------------------------------
--
--  Public interface
--
--------------------------------

function process_message()
    if config.max_timestamp == -1 then return 0 end

    -- Given a message's timestamp in nanoseconds, convert to seconds
    local msg_time_seconds = math.floor(read_message("Timestamp") / 1e9)
    if msg_time_seconds >= config.max_timestamp then
        write_message("Type", "MaxTimestamp")
    end
    return 0
end
