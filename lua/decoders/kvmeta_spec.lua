-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local cjson = require 'cjson'
local util = require 'util'

describe("KV Decoder", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_cfg = {}
    local mock_kvmeta = {
        kv_version = "1.2.3",
        kv_language = "go",
        team = "eng-team",
        routes = {
            {
                rule="rule-1-alerts",
                type="alerts",
                series="series_1",
                value="value_a",
                dimensions={},
            },
            {
                rule="rule-2-metrics",
                type="metrics",
                series="series_b",
                dimensions={"custom_dim1", "custom_dim2"},
            },
        }
    }
    local mock_msg = {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = "hostname"
    mock_msg.Fields = {}
    mock_msg.Fields.series_a = "series-name-b"
    mock_msg.Fields.series_b = "series-name-b"
    mock_msg.Fields.value_a = 100
    mock_msg.Fields.value_b = 200
    mock_msg.Fields.custom_dim1 = "custom_value"
    mock_msg.Fields.custom_dim2 = "custom_value"
    mock_msg.Fields._kvmeta = cjson.encode(mock_kvmeta)

    function test_setup()
        mocks.reset()
        mocks.set_config(mock_cfg)
        util.unrequire('kvmeta')
        require('kvmeta')
        mocks.set_next_message(mock_msg)
    end

    it("should error if _kvmeta field is not present", function()
        test_setup()
        local msg = util.deepcopy(mock_msg)
        msg.Fields._kvmeta = nil
        mocks.set_next_message(msg)
        assert.equals(-1, process_message(), "process_message should error")
    end)

    it("should inject original message and remove _kvmeta field", function()
        test_setup()
        local msg = util.deepcopy(mock_msg)
        local kvmeta = util.deepcopy(mock_kvmeta)
        kvmeta.routes = {
            {
                rule="rule-1-alerts",
                type="alerts",
                series="series_1",
                value="value_a",
                dimensions={},
            },
        }
        msg.Fields._kvmeta = cjson.encode(kvmeta)
        mocks.set_next_message(msg)

        assert.equals(0, process_message(), "process_message should succeed")
        injected = mocks.injected_messages()
        assert.equals(2, #injected, "Correct number of Heka messages were inserted")

        expected_msg1 = util.deepcopy(msg)
        expected_msg1.Fields._kvmeta = nil
        expected_msg1.Fields["_kvmeta.type"] = "logs"
        expected_msg1.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg1.Fields["_kvmeta.kv_language"] = "go"
        expected_msg1.Fields["_kvmeta.team"] = "eng-team"
        expected_msg1.Fields["_kvmeta.route-rules"] = "rule-1-alerts"
        assert.same(expected_msg1, injected[1])

        expected_msg2 = util.deepcopy(mock_msg)
        expected_msg2.Fields._kvmeta = nil
        expected_msg2.Fields["_kvmeta.rule"] = "rule-1-alerts"
        expected_msg2.Fields["_kvmeta.type"] = "alerts"
        expected_msg2.Fields["_kvmeta.series"] = "series_1"
        expected_msg2.Fields["_kvmeta.value"] = "value_a"
        expected_msg2.Fields["_kvmeta.dimensions"] = ""
        expected_msg2.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg2.Fields["_kvmeta.kv_language"] = "go"
        expected_msg2.Fields["_kvmeta.team"] = "eng-team"
        assert.same(expected_msg2, injected[2])
    end)

    it("should inject updated message for each rule in _kvmeta", function()
        test_setup()

        process_result = process_message()
        assert.equals(0, process_result, "process_message should succeed")
        injected = mocks.injected_messages()
        assert.equals(3, #injected, "Correct number of Heka messages were inserted")

        expected_msg1 = util.deepcopy(mock_msg)
        expected_msg1.Fields._kvmeta = nil
        expected_msg1.Fields["_kvmeta.type"] = "logs"
        expected_msg1.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg1.Fields["_kvmeta.kv_language"] = "go"
        expected_msg1.Fields["_kvmeta.team"] = "eng-team"
        expected_msg1.Fields["_kvmeta.route-rules"] = "rule-1-alerts rule-2-metrics"
        assert.same(expected_msg1, injected[1])

        expected_msg2 = util.deepcopy(mock_msg)
        expected_msg2.Fields._kvmeta = nil
        expected_msg2.Fields["_kvmeta.rule"] = "rule-1-alerts"
        expected_msg2.Fields["_kvmeta.type"] = "alerts"
        expected_msg2.Fields["_kvmeta.series"] = "series_1"
        expected_msg2.Fields["_kvmeta.value"] = "value_a"
        expected_msg2.Fields["_kvmeta.dimensions"] = ""
        expected_msg2.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg2.Fields["_kvmeta.kv_language"] = "go"
        expected_msg2.Fields["_kvmeta.team"] = "eng-team"
        assert.same(expected_msg2, injected[2])

        expected_msg3 = util.deepcopy(mock_msg)
        expected_msg3.Fields._kvmeta = nil
        expected_msg3.Fields["_kvmeta.rule"] = "rule-2-metrics"
        expected_msg3.Fields["_kvmeta.type"] = "metrics"
        expected_msg3.Fields["_kvmeta.series"] = "series_b"
        expected_msg3.Fields["_kvmeta.dimensions"] = "custom_dim1 custom_dim2"
        expected_msg3.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg3.Fields["_kvmeta.kv_language"] = "go"
        expected_msg3.Fields["_kvmeta.team"] = "eng-team"
        assert.same(expected_msg3, injected[3])
    end)

    it("should set message Type if `type` in config", function()
        -- Test setup
        mocks.reset()
        local cfg =  util.deepcopy(mock_cfg)
        cfg['msg_type'] = 'kvmeta' -- we expect this to be set on the injected message
        mocks.set_config(cfg)
        util.unrequire('kvmeta')
        require 'kvmeta'

        local msg = util.deepcopy(mock_msg)
        local kvmeta = util.deepcopy(mock_kvmeta)
        kvmeta.routes = {
            {
                rule="rule-1-alerts",
                type="alerts",
                series="series_1",
                value="value_a",
                dimensions={},
            },
        }
        msg.Fields._kvmeta = cjson.encode(kvmeta)
        mocks.set_next_message(msg)

        -- Test
        assert.equals(0, process_message(), "process_message should succeed")
        injected = mocks.injected_messages()
        assert.equals(2, #injected, "Correct number of Heka messages were inserted")

        expected_msg1 = util.deepcopy(msg)
        expected_msg1.Fields._kvmeta = nil
        expected_msg1.Fields["_kvmeta.type"] = "logs"
        expected_msg1.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg1.Fields["_kvmeta.kv_language"] = "go"
        expected_msg1.Fields["_kvmeta.team"] = "eng-team"
        expected_msg1.Fields["_kvmeta.route-rules"] = "rule-1-alerts"
        expected_msg1.Type = "kvmeta"
        assert.same(expected_msg1, injected[1])

        expected_msg2 = util.deepcopy(mock_msg)
        expected_msg2.Fields._kvmeta = nil
        expected_msg2.Fields["_kvmeta.rule"] = "rule-1-alerts"
        expected_msg2.Fields["_kvmeta.type"] = "alerts"
        expected_msg2.Fields["_kvmeta.series"] = "series_1"
        expected_msg2.Fields["_kvmeta.value"] = "value_a"
        expected_msg2.Fields["_kvmeta.dimensions"] = ""
        expected_msg2.Fields["_kvmeta.kv_version"] = "1.2.3"
        expected_msg2.Fields["_kvmeta.kv_language"] = "go"
        expected_msg2.Fields["_kvmeta.team"] = "eng-team"
        expected_msg2.Type = "kvmeta"
        assert.same(expected_msg2, injected[2])

    end)

    it("if no routes, should set message Type to 'Kayvee' for backwards compatibility", function()
        -- Test setup
        mocks.reset()
        local cfg =  util.deepcopy(mock_cfg)
        cfg['msg_type'] = 'kvmeta' -- we expect this to be set on the injected message
        mocks.set_config(cfg)
        util.unrequire('kvmeta')
        require 'kvmeta'

        local msg = util.deepcopy(mock_msg)
        local kvmeta = util.deepcopy(mock_kvmeta)
        kvmeta.routes = {}
        msg.Fields._kvmeta = cjson.encode(kvmeta)
        mocks.set_next_message(msg)

        -- Test
        assert.equals(0, process_message(), "process_message should succeed")
        injected = mocks.injected_messages()
        assert.equals(0, #injected, "Correct number of Heka messages were inserted")

        written = mocks.written_messages()
        assert.same("Kayvee", written["Type"])
        assert.same("__REMOVED_FIELD__", written["Fields[_kvmeta]"])
    end)
end)
