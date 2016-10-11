-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'
local cjson = require 'cjson'

describe("Kayvee Signalfx Batch Filter", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_cfg = {
        series_field="series",
        value_ref_field="value_f",
        stat_type_field="stat_type",
        dimensions_field="dimensions",
        default_dimensions="Hostname",
    }

    local mock_msg = {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = "hostname"
    mock_msg.Fields = {}
    mock_msg.Fields.series = "series-name"
    mock_msg.Fields.value_f = "value"
    mock_msg.Fields.value = 100
    mock_msg.Fields.stat_type = "counter"
    mock_msg.Fields.dimensions = "custom_dim"
    mock_msg.Fields.custom_dim = "custom_value"

    function test_setup()
        mocks.reset()
        mocks.set_config(mock_cfg)
        require 'kayvee_signalfxbatch'
        mocks.set_next_message(mock_msg)
    end

    it("should process and flush one message", function()
        test_setup()

        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert(#injected == 1, "There should be one Heka message injected")

        decoded = cjson.decode(injected[1]["data"])
        assert(decoded["counter"][1]["timestamp"] == 2)
        assert(decoded["counter"][1]["metric"] == "series-name")
        assert(decoded["counter"][1]["value"] == 100)
        assert.same({Hostname="hostname",custom_dim="custom_value",}, decoded["counter"][1]["dimensions"])
    end)

    it("should process and flush a message with no dimensions", function()
        test_setup()

        local msg = util.deepcopy(mock_msg)
        msg.Fields.dimensions = ""
        mocks.set_next_message(msg)

        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert(#injected == 1, "There should be one Heka message injected")

        decoded = cjson.decode(injected[1]["data"])
        assert.same({Hostname="hostname"}, decoded["counter"][1]["dimensions"])
    end)

    it("should batch two messages", function()
        test_setup()

        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
        decoded = cjson.decode(injected[1]["data"])
        assert.equals(#decoded["counter"], 2, "There should be 2 messages in the batched payload")
    end)

    it("should label gauges separately from counters", function()
        test_setup()

        local mock_gauge = util.deepcopy(mock_msg)
        mock_msg.Fields.stat_type = "gauge"
        mocks.set_next_message(mock_msg)
        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
        decoded = cjson.decode(injected[1]["data"])
        assert(decoded["counter"] == nil, "Should have gauges, and no counter")
        expected_gauge = {
            timestamp = 2,
            metric = "series-name",
            value = 100,
            dimensions = {
                Hostname = "hostname",
                custom_dim="custom_value",
            },
        }
        assert.same(decoded.gauge[1], expected_gauge)
    end)

    it("should flush() messages on a timer_event", function()
        test_setup()

        -- Test
        assert.equals(0, process_message(), "Should process_message successfully")
        timer_event()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
    end)
end)
