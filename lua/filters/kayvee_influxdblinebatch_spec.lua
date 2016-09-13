-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'
local cjson = require 'cjson'

require 'kayvee_influxdblinebatch'

-- returns the number of lines in the msg "data"
function line_count(msg)
    -- there's always a trailing "\n" we don't want to count
    return #util.lines(msg.data) - 1
end

describe("Kayvee Influxdbline Batch Filter", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_cfg = {
		name = "name",
		tag_fields = "env Hostname",
		skip_fields = "**all_base** Pid Type Logger Severity programname test title source level env type _prefix _postfix",
        -- "**all_base** Pid Type Logger Severity programname test title source level env type _prefix _postfix",
		max_count = 100,

        --series_field="series_f",
        --value_field="value_f",
        --stat_type_field="stat_type_f",
        --dimensions_field="dimensions_f",
        --default_dimensions="Hostname",
    }

    local mock_msg= {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = "hostname"
    mock_msg['env'] = "test"
    --mock_msg["Fields[series_f]"] = "series"
    --mock_msg["Fields[series]"] = "series-name"
    --mock_msg["Fields[value_f]"] = "value"
    --mock_msg["Fields[value]"] = 100
    --mock_msg["Fields[stat_type_f]"] = "stat_type"
    --mock_msg["Fields[stat_type]"] = "counter"
    --mock_msg["Fields[dimensions_f]"] = "dimensions"
    --mock_msg["Fields[dimensions]"] = "custom_dim"
    --mock_msg["Fields[custom_dim]"] = "custom_value"
    --expected_dimensions = {
        --Hostname="hostname",
        --custom_dim="custom_value",
    --}

    it("should process and flush one message", function()
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
		configure()
        mocks.set_next_message(mock_msg)

        -- Test
        assert.equals(process_message(), 0, "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1)
        actual_msg = injected[1]
        assert.equals(1, line_count(actual_msg), "Incorrect number of Heka messages batched in payload")
        assert.equals("influxdblinebatch", actual_msg.payload_name)
        assert.equals("txt", actual_msg.payload_type)
        --assert.equals("name,env=test,Hostname=hostname Timestamp=2000000.000000 2\n", actual_msg.data)
        assert.equals("name,Hostname=hostname,env=test Timestamp=2000000.000000 2\n", actual_msg.data)
    end)

    it("should batch messages", function()
        -- Test setup
        mocks.reset()
        mocks.set_config(mock_cfg)
        configure()
        mocks.set_next_message(mock_msg)

        -- Test
        -- Should batch 3 messages in one payload
        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(1, #injected, "Incorrect number of Heka messages injected")
        assert.equals(3, line_count(injected[1]), "Incorrect number of Heka messages batched in payload")

        -- Should batch 4 messages in one payload
        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        assert.equals(0, process_message(), "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(2, #injected, "Incorrect number of Heka messages injected")
        assert.equals(4, line_count(injected[2]), "Incorrect number of Heka messages batched in payload")
    end)

    --it("should read message name from message data", function()
        ---- Test setup
        --mocks.reset()
        --mocks.set_config(mock_cfg)
        --configure()
        --mock_msg = util.deepcopy(mock_msg)
        --mocks.set_next_message(mock_msg)

        ---- Test
        --assert.equals(process_message(), 0, "Should process_message successfully")
        --flush()
        --injected = mocks.injected_payloads()
        --assert.equals(#injected, 1)
        --actual_msg = injected[1]
        --assert.equals(1, line_count(actual_msg), "Incorrect number of Heka messages batched in payload")
        --assert.equals("influxdblinebatch", actual_msg.payload_name)
        --assert.equals("txt", actual_msg.payload_type)
        --assert.equals("name,Hostname=hostname,env=test Timestamp=2000000.000000 2\n", actual_msg.data)
        ----series_field="series_f",

        --mocks.set_next_message(mock_msg)
        ---- Test
        ----assert.equals(actual_msg.data, "name,Hostname=hostname Timestamp=2000000.000000 2\n")
    --end)

    --it("should label gauges separately from counters", function()
        ---- Test setup
        --mocks.reset()
        --mocks.set_config(mock_cfg)
        --configure()
        --mocks.set_next_message(mock_msg)

        ---- Test
        --local mock_gauge = util.deepcopy(mock_msg)
        --mock_msg["Fields[stat_type]"] = "gauge"
        --mocks.set_next_message(mock_msg)
        --process_result = process_message()
        --assert.equals(process_result, 0, "Should process_message successfuly")
        --flush()
        --injected = mocks.injected_payloads()
        --assert.equals(#injected, 1, "There should be one Heka message injected")
        --decoded = cjson.decode(injected[1]["data"])
        --assert(decoded["counter"] == nil, "Should have gauges, and no counter")
        --expected_gauge = {
            --timestamp = 2,
            --metric = "series-name",
            --value = 100,
            --dimensions = {
                --Hostname = "hostname",
                --custom_dim="custom_value",
            --},
        --}
        --assert.same(decoded.gauge[1], expected_gauge)
    --end)

    --it("should flush() messages on a timer_event", function()
        ---- Test setup
        --mocks.reset()
        --mocks.set_config(mock_cfg)
        --configure()
        --mocks.set_next_message(mock_msg)

        ---- Test
        --process_result = process_message()
        --assert.equals(process_result, 0, "Should process_message successfuly")
        --timer_event()
        --injected = mocks.injected_payloads()
        --assert.equals(#injected, 1, "There should be one Heka message injected")
     --end)
end)
