-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local cjson = require 'cjson'
local util = require 'util'

describe("KV Decoder", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_cfg = {
        series_field="series_f",
        value_field="value_f",
        stat_type_field="stat_type_f",
        dimensions_field="dimensions_f",
        default_dimensions="Hostname",
    }

    local mock_msg = {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = "hostname"
    mock_msg.Fields = {}
    mock_msg.Fields.series_f = "series"
    mock_msg.Fields.series = "series-name"
    mock_msg.Fields.value_f = "value"
    mock_msg.Fields.value = 100
    mock_msg.Fields.stat_type_f = "stat_type"
    mock_msg.Fields.stat_type = "counter"
    mock_msg.Fields.dimensions_f = "dimensions"
    mock_msg.Fields.dimensions = "custom_dim"
    mock_msg.Fields.custom_dim = "custom_value"
    mock_msg.Fields._kvmeta = cjson.encode({
        {
            rule="rule-1",
            type="type-1",
            series="series-1",
        },
        {
            rule="rule-2",
            type="type-2",
            series="series-2",
        },
    })


    function test_setup()
        mocks.reset()
        mocks.set_config(mock_cfg)
        require 'kvmeta'
        mocks.set_next_message(mock_msg)
    end

    it("should inject original message and remove _kvmeta field", function()
        test_setup()
        local msg = util.deepcopy(mock_msg)
        msg.Fields._kvmeta = cjson.encode({})
        mocks.set_next_message(msg)

        assert.equals(0, process_message(), "Should process_message successfully")
        injected = mocks.injected_messages()
        assert.equals(1, #injected, "Correct number of Heka messages were inserted")

        expected_msg1 = util.deepcopy(msg)
        expected_msg1["Fields[_kvmeta]"] = nil
        assert.same(expected_msg1, injected[1])
    end)


    it("should inject original message for each rule in _kvmeta", function()
        test_setup()

        process_result = process_message()
        assert.equals(0, process_result, "Should process_message successfully")
        injected = mocks.injected_messages()
        assert.equals(3, #injected, "Correct number of Heka messages were inserted")

        expected_msg1 = util.deepcopy(mock_msg)
        expected_msg1.Fields._kvmeta = nil
        assert.same(expected_msg1, injected[1])

        expected_msg2 = util.deepcopy(mock_msg)
        expected_msg2.Fields._kvmeta = nil
        expected_msg2.Fields["_kvmeta.rule"] = "rule-1"
        expected_msg2.Fields["_kvmeta.type"] = "type-1"
        expected_msg2.Fields["_kvmeta.series"] = "series-1"
        assert.same(expected_msg2, injected[2])

        expected_msg3 = util.deepcopy(mock_msg)
        expected_msg3.Fields._kvmeta = nil
        expected_msg3.Fields["_kvmeta.rule"] = "rule-2"
        expected_msg3.Fields["_kvmeta.type"] = "type-2"
        expected_msg3.Fields["_kvmeta.series"] = "series-2"
        assert.same(expected_msg3, injected[3])
    end)

end)
