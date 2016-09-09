---------------------------------------
-- Global Utils
---------------------------------------

local module = {}

function module.shallow_compare(t1, t2)
    if #t1 - #t2 ~= 0 then
        return false
    end

    for k, v in pairs(t1) do
        if t1[k] ~= t2[k] then
            debug("shallow_compare: key '" .. k .. "' not equal. (t1: " .. t1[k] .. ", t2: " .. t2[k] .. ")")
            return false
        end
    end

    return true
end

-- http://lua-users.org/wiki/CopyTable
function module.deepcopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[module.deepcopy(orig_key)] = module.deepcopy(orig_value)
        end
        setmetatable(copy, module.deepcopy(getmetatable(orig)))
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

return module
