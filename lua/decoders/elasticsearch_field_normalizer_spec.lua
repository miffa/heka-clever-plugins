-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'

describe("Elasticsearch field normalizer", function()
    it("No fields should be changed on a standard log line", function()
        mocks.reset()
        require 'elasticsearch_field_normalizer'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.env = "test"
        mock_msg.Fields.series_f = "series"
        mock_msg.Fields.series = "series-name"
        mock_msg.Fields.value_f = "value"
        mock_msg.Fields.value = 100
        mock_msg.Fields.dimensions_f = "dimensions"
        mock_msg.Fields.dimensions = ""
        mock_msg.Fields.custom_dim = "custom_value"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        for k, v in pairs(written_messages) do
           assert(False, "No values should be changed")
        end
    end)
    it("Elasticserach metafields should be replaced", function()
        mocks.reset()
        require 'elasticsearch_field_normalizer'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields._meta = "test"
        mock_msg.Fields._timestamp = "test"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("__REMOVED_FIELD__", written_messages["Fields[_meta]"])
        assert.equals("test", written_messages["Fields[kv__meta]"])
        assert.equals("__REMOVED_FIELD__", written_messages["Fields[_timestamp]"])
        assert.equals("test", written_messages["Fields[kv__timestamp]"])
    end)
    it("Dots should be replaced with underscores", function()
        mocks.reset()
        require 'elasticsearch_field_normalizer'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields["params.District"] = "test"
        mock_msg.Fields["params.Foop"] = "test"
        mock_msg.Fields["params.StartingAfter"] = "test"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("__REMOVED_FIELD__", written_messages["Fields[params.District]"])
        assert.equals("test", written_messages["Fields[params_District]"])
        assert.equals("__REMOVED_FIELD__", written_messages["Fields[params.Foop]"])
        assert.equals("test", written_messages["Fields[params_Foop]"])
    end)
end)
