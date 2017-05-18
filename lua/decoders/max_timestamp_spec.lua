-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'

describe("MaxTimestamp Decoder", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_unix_timestamp = 1495057242 -- Wed May 17 14:40:42 2017
    local mock_unix_timestamp_ns = mock_unix_timestamp * 1e9
    local mock_msg = {}
    mock_msg.Fields = {}

    local cfg = {}
    cfg.max_timestamp = mock_unix_timestamp

    function test_setup(cfg)
        mocks.reset()
        mocks.set_config(cfg)
        util.unrequire('max_timestamp')
        require('max_timestamp')
        mocks.set_next_message(mock_msg)
    end

    it("should *not* set message Type if message timestamp is < max_timetamp", function()
        test_setup(cfg)

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = mock_unix_timestamp_ns - 1e9 -- 1 second earlier
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        written_messages = mocks.written_messages()
        assert.equals(nil, written_messages["Type"], "message Type should *not* be updated")
    end)

    it("should set message Type=='MaxTimestamp' if message timestamp is == max_timetamp", function()
        test_setup(cfg)

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = mock_unix_timestamp_ns
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        written_messages = mocks.written_messages()
        assert.equals("MaxTimestamp", written_messages["Type"], "message Type should be written")
    end)

    it("should set message Type=='MaxTimestamp' if message timestamp is > max_timetamp", function()
        test_setup(cfg)

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = mock_unix_timestamp_ns + 1e9 -- 1 second later
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        written_messages = mocks.written_messages()
        assert.equals("MaxTimestamp", written_messages["Type"], "message Type should be written")
    end)

    it("works if max_timestamp config is passed as a string", function()
        local new_cfg = util.deepcopy(mock_msg)
        new_cfg.max_timestamp = "1495057242"
        test_setup(new_cfg)

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = mock_unix_timestamp_ns + 1e9 -- 1 second later
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        written_messages = mocks.written_messages()
        assert.equals("MaxTimestamp", written_messages["Type"], "message Type should be written")
    end)

    it("if max_timestamp == -1, then the decoder is ignored", function()
        local new_cfg = util.deepcopy(mock_msg)
        new_cfg.max_timestamp = "-1"
        test_setup(new_cfg)

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = mock_unix_timestamp_ns + 1e9 -- 1 second later
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        written_messages = mocks.written_messages()
        assert.equals(nil, written_messages["Type"], "message Type should be written")
    end)

end)
