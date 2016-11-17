--[=[

Throws away any message with a timestamp > 2 weeks ago.

--]=]

local os = require 'os'

-- Read and validate config for `max_age_in_days`
local max_age_in_days = read_config("max_age_in_days") or error("max_age_in_days must be set in config")
max_age_in_days = tonumber(max_age_in_days) or error("max_age_in_days must be a number")
if not (max_age_in_days > 0) then error("max_age_in_days must be > 0") end

local config = {
    max_age_in_days = max_age_in_days
}

--------------------------------
--
--  Public interface
--
--------------------------------

function process_message()
    local msg_time_seconds = math.floor(read_message("Timestamp") / 1e9)
    local minimum_timestamp_allowed = os.time() - (config.max_age_in_days*24*60*60) -- # days, 24 hours, 60 minutes, 60 seconds

    if msg_time_seconds < minimum_timestamp_allowed then return -1 end

    return 0
end
