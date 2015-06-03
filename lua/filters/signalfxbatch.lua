cjson = require "cjson"
require "string"
require "table"
package.path = package.path .. ";../?.lua" -- Allow requiring from parent folder
util = require "modules.util"

local metric_name = read_config("metric_name")
local value_field = read_config("value_field") or "value"
local dimensions_str = read_config("dimensions") or ""
local msg_type = read_config("msg_type") or "signalfxbatch"

local use_subs
if string.find(metric_name, "%%{[%w%p]-}") then
    use_subs = true
end

local function get_dimensions(s)
  local dims = {}
  -- TODO: make sure matcher supports all possible field names
  for i in string.gmatch(s, "%S+") do
    dims[i] = util.sub_func(i)
  end
  return dims
end

local counters = {}
local gauges = {}
batch_max_count = read_config("max_count") or 20

function flush()
   if #counters > 0 or #gauges > 0 then
      local output = {
        counter = { counters },
        gauge = { gauges }
      }
      inject_payload("json", msg_type, cjson.encode(output))
      counters = {}
      gauges = {}
   end
   return 0
end

function process_message()
    local ts = tonumber(read_message("Timestamp"))
    if not ts then return -1 end
    ts = ts / 1e6 -- Convert nanoseconds to milliseconds

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
        name = string.gsub(metric_name, "%%{([%w%p]-)}", util.sub_func)
    else
        name = metric_name
    end
    if not name or name == "" then return -1 end

    -- Read custom dimensions from message
    local dims = get_dimensions(dimensions_str)

    -- single data point
    local datum = {
        metric=name,
        value=value,
        timestamp=ts,
        dimensions=dims
    }

    -- Batch data points, grouping by 'counter' or 'gauge'
    if stat_type == 'counter' then
        table.insert(counters, datum)
    elseif stat_type == 'gauge' then
        table.insert(gauges, datum)
    else
        return -1
    end

    if #counters + #gauges == batch_max_count then
        flush()
    end
    return 0
end

function timer_event(ns)
    flush()
end
