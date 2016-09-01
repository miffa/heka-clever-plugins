--[[
Elasticsearch rejects all messags with certain metafields.  This decoder ensures that all
messages don't have these metafields and renames them if they do.

es metafields:
https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html
--]]

require "string"

local field_renames = {
    _index = "kv._index",
    _uid = "kv._uid",
    _type = "kv._type",
    _id = "kv._id",
    _source = "kv._source",
    _size = "kv._size",
    _all = "kv._all",
    _field_names = "kv._field_names",
    _timestamp = "kv._timestamp",
    _ttl = "kv._ttl",
    _parent = "kv._parent",
    _routing = "kv._routing",
    _meta = "kv._meta"
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
