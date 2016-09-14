-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'
local cjson = require 'cjson'


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

        skip_fields = "**all_base** Timestamp Pid Type Logger Severity programname test title source level env type _prefix _postfix",
        max_count = 100,

        series_field="series_f",
        value_field="value_f",
        dimensions_field="dimensions_f",
        default_dimensions="Hostname env",
    }

    local mock_msg= {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = "hostname"
    mock_msg['Fields[env]'] = "test"
    mock_msg["Fields[series_f]"] = "series"
    mock_msg["Fields[series]"] = "series-name"
    mock_msg["Fields[value_f]"] = "value"
    mock_msg["Fields[value]"] = 100
    mock_msg["Fields[dimensions_f]"] = "dimensions"
    mock_msg["Fields[dimensions]"] = ""
    mock_msg["Fields[custom_dim]"] = "custom_value"

    function test_setup()
        mocks.reset()
        mocks.set_config(mock_cfg)
        require 'kayvee_influxdblinebatch'
        mocks.set_next_message(mock_msg)
    end

    it("should process and flush one message", function()
        test_setup()

        assert.equals(process_message(), 0, "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1)
        actual_msg = injected[1]
        assert.equals(1, line_count(actual_msg), "Incorrect number of Heka messages batched in payload")
        assert.equals("influxdblinebatch", actual_msg.payload_name)
        assert.equals("txt", actual_msg.payload_type)
        assert.equals("series-name,Hostname=hostname,env=test value=100.000000 2\n", actual_msg.data)
    end)

    it("should batch messages", function()
        test_setup()

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

    it("should read series name from specified field", function()
        -- Test setup
        test_setup()
        mock_msg_new = util.deepcopy(mock_msg)
        mock_msg_new["Fields[series]"] = "series-name-new"
        mocks.set_next_message(mock_msg_new)

        -- Test
        assert.equals(process_message(), 0, "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1)
        actual_msg = injected[1]
        assert.equals("series-name-new,Hostname=hostname,env=test value=100.000000 2\n", actual_msg.data)
    end)

    it("should read value from specified field", function()
        -- Test setup
        test_setup()
        mock_msg_new = util.deepcopy(mock_msg)
        mock_msg_new["Fields[value]"] = 999
        mocks.set_next_message(mock_msg_new)

        -- Test
        assert.equals(process_message(), 0, "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1)
        actual_msg = injected[1]
        assert.equals("series-name,Hostname=hostname,env=test value=999.000000 2\n", actual_msg.data)
    end)

    it("should read dimensions from specified field", function()
        -- Test setup
        test_setup()
        mock_msg_new = util.deepcopy(mock_msg)
        mock_msg_new["Fields[dimensions]"] = "custom_dim custom_dim2"
        mock_msg_new["Fields[custom_dim]"] = "aaa"
        mock_msg_new["Fields[custom_dim2]"] = "bbb"
        mocks.set_next_message(mock_msg_new)

        -- Test
        assert.equals(process_message(), 0, "Should process_message successfully")
        flush()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1)
        actual_msg = injected[1]
        assert.equals("series-name,Hostname=hostname,custom_dim=aaa,custom_dim2=bbb,env=test value=100.000000 2\n", actual_msg.data)
    end)


    it("should flush() messages on a timer_event", function()
        test_setup()

        process_result = process_message()
        assert.equals(process_result, 0, "Should process_message successfuly")
        timer_event()
        injected = mocks.injected_payloads()
        assert.equals(#injected, 1, "There should be one Heka message injected")
     end)
end)
