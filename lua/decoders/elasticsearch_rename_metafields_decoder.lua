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
    for field, rename in pairs(field_renames) do
        local val = read_message("Fields["..field.."]")

        if val ~= nil then -- Rename field if it exists
            write_message("Fields["..field.."]", nil) -- Setting to nil deletes field
            write_message("Fields["..rename.."]", val)
        end
    end

    return 0
end
