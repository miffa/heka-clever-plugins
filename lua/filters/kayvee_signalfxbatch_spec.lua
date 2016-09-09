-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'
local cjson = require 'cjson'
require 'kayvee_signalfxbatch'

describe("Kayvee Signalfx Batch Filter", function()
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
    mock_msg["Fields[series_f]"] = "series"
    mock_msg["Fields[series]"] = "series-name"
    mock_msg["Fields[value_f]"] = "value"
    mock_msg["Fields[value]"] = 100
    mock_msg["Fields[stat_type_f]"] = "stat_type"
    mock_msg["Fields[stat_type]"] = "counter"
    mock_msg["Fields[dimensions_f]"] = "dimensions"
    mock_msg["Fields[dimensions]"] = "custom_dim"
    mock_msg["Fields[custom_dim]"] = "custom_value"
    expected_dimensions = {
        Hostname="hostname",
        custom_dim="custom_value",
    }

    it("should process and flush one message", function()
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
        configure()
        mocks.set_next_message(mock_msg)

        -- Test
        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
        flush()
        injected = mocks.injected_payloads()
        assert(#injected == 1, "There should be one Heka message injected")

        decoded = cjson.decode(injected[1]["data"])
        assert(decoded["counter"][1]["timestamp"] == 2)
        assert(decoded["counter"][1]["metric"] == "series-name")
        assert(decoded["counter"][1]["value"] == 100)
        assert(decoded["counter"][1]["dimensions"]["Hostname"] == "hostname")
        assert(util.shallow_compare(decoded["counter"][1]["dimensions"], expected_dimensions))
        assert.same(decoded["counter"][1]["dimensions"], expected_dimensions)
    end)

    it("should batch two messages", function()
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
        configure()
        mocks.set_next_message(mock_msg)

        -- Test
        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
        decoded = cjson.decode(injected[1]["data"])
        assert.equals(#decoded["counter"], 2, "There should be 2 messages in the batched payload")
    end)

    it("should label gauges separately from counters", function()
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
        configure()
        mocks.set_next_message(mock_msg)

        -- Test
        local mock_gauge = util.deepcopy(mock_msg)
        mock_msg["Fields[stat_type]"] = "gauge"
        mocks.set_next_message(mock_msg)
        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
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
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
        configure()
        mocks.set_next_message(mock_msg)

        -- Test
        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
        timer_event()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
    end)
end)
