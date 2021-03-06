---------------------------------------
-- Global Utils
---------------------------------------

local module = {}

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

-- https://coronalabs.com/blog/2014/09/02/tutorial-printing-table-contents/
function module.print_r( t )
    local print_r_cache={}
    local function sub_print_r(t,indent)
        if (print_r_cache[tostring(t)]) then
            print(indent.."*"..tostring(t))
        else
            print_r_cache[tostring(t)]=true
            if (type(t)=="table") then
                for pos,val in pairs(t) do
                    if (type(val)=="table") then
                        print(indent.."["..pos.."] => "..tostring(t).." {")
                        sub_print_r(val,indent..string.rep(" ",string.len(pos)+8))
                        print(indent..string.rep(" ",string.len(pos)+6).."}")
                    elseif (type(val)=="string") then
                        print(indent.."["..pos..'] => "'..val..'"')
                    else
                        print(indent.."["..pos.."] => "..tostring(val))
                    end
                end
            else
                print(indent..tostring(t))
            end
        end
    end
    if (type(t)=="table") then
        print(tostring(t).." {")
        sub_print_r(t,"  ")
        print("}")
    else
        sub_print_r(t,"  ")
    end
    print()
end

-- Split multiline string into lines
-- http://lua-users.org/wiki/SplitJoin
function module.lines(str)
  local t = {}
  local function helper(line) table.insert(t, line) return "" end
  helper((str:gsub("(.-)\r?\n", helper)))
  return t
end

-- Unload a module.
-- We need this so we can require a Heka lua plugin multiple times, with different config.
-- http://lua-users.org/lists/lua-l/2009-03/msg00587.html
function module.unrequire(m)
    package.loaded[m] = nil
    _G[m] = nil
end

return module
