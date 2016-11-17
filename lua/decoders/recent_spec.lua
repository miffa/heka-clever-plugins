-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'

describe("Recent Decoder", function()
    -- Prep mocks, which are re-used in multiple tests
    local mock_msg = {}
    mock_msg['Timestamp'] = os.time() * 1e9 -- current time
    mock_msg.Fields = {}

    function test_setup(cfg)
        mocks.reset()
        mocks.set_config(cfg)
        util.unrequire('recent')
        require('recent')
        mocks.set_next_message(mock_msg)
    end

    it("should succeed/error based on max_age_in_days (14 days)", function()
        local cfg = {}
        cfg.max_age_in_days = 14
        test_setup(cfg)

        assert.equals(0, process_message(), "process_message should succeed")  -- 0 days ago

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = os.time() * 1e9 - (13*24*60*60) * 1e9 -- 13 days
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = os.time() * 1e9 - (15*24*60*60) * 1e9 -- 15 days
        mocks.set_next_message(msg)
        assert.equals(-1, process_message(), "process_message should error")
    end)

    it("should succeed/error based on max_age_in_days (2 days)", function()
        local cfg = {}
        cfg.max_age_in_days = 2
        mocks.set_config(cfg)
        test_setup(cfg)

        assert.equals(0, process_message(), "process_message should succeed")  -- 0 days ago

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = os.time() * 1e9 - (1*24*60*60) * 1e9 -- 1 day ago
        mocks.set_next_message(msg)
        assert.equals(0, process_message(), "process_message should succeed")

        local msg = util.deepcopy(mock_msg)
        msg['Timestamp'] = os.time() * 1e9 - (3*24*60*60) * 1e9 -- 3 days ago
        mocks.set_next_message(msg)
        assert.equals(-1, process_message(), "process_message should error")
    end)
end)
