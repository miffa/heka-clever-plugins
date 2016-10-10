--[[
Elasticsearch rejects all messags with certain metafields.  This decoder ensures that all
messages don't have these metafields and renames them if they do.

es metafields:
https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html
--]]

require "string"

local field_renames = {
    _index = "kv__index",
    _uid = "kv__uid",
    _type = "kv__type",
    _id = "kv__id",
    _source = "kv__source",
    _size = "kv__size",
    _all = "kv__all",
    _field_names = "kv__field_names",
    _timestamp = "kv__timestamp",
    _ttl = "kv__ttl",
    _parent = "kv__parent",
    _routing = "kv__routing",
    _meta = "kv__meta"
}

function process_message()
    toRename = {}

    while true do
        local typ, name, value, representation, count = read_next_field()

        if not typ then break end

        if typ ~= 1 then -- exclude bytes
            local rename = name

            if field_renames[name] ~= nil then
                rename = field_renames[name]
            elseif name:find("%.") ~= nil then
                rename = name:gsub("%.", "_")
            end

            if name ~= rename then
                toRename[#toRename+1] = { old=name, new=rename, value=value }
            end
        end
    end

    for i, rename in ipairs(toRename) do
        write_message("Fields["..rename.old.."]", nil) -- Setting to nil deletes field
        write_message("Fields["..rename.new.."]", rename.value)
    end

    return 0
end
