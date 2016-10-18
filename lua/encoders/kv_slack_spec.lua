-- Allow importing from parent directory
package.path = package.path .. ';../?.lua'

local mocks = require 'mocks'
local cjson = require 'cjson'
local util = require 'util'

describe('KV Slack Encoder', function()
    -- Prep mocks, which may be re-used in multiple tests
    local mock_cfg = {
        message_field='_kvmeta.message',
        channel_field='_kvmeta.channel',
        icon_field='_kvmeta.icon',
        username_field='_kvmeta.user',
    }
    local mock_msg = {}
    mock_msg['Timestamp'] = 2000000
    mock_msg['Hostname'] = 'hostname'
    mock_msg.Fields = {
        ['_kvmeta.type'] = 'notifications',
        ['_kvmeta.channel'] = '#team',
        ['_kvmeta.icon'] = ':rocket:',
        ['_kvmeta.message'] = 'hello world!',
        ['_kvmeta.user'] = '@user',
    }

    function test_setup()
        mocks.reset()
        mocks.set_config(mock_cfg)
        util.unrequire('kv_slack')
        require 'kv_slack'
        mocks.set_next_message(mock_msg)
    end

    it('should pass through field values', function()
        test_setup()
        assert.equals(0, process_message(), 'process_message should error')

        local injected = mocks.injected_payloads()
        assert.equals(1, #injected, "There should be one Heka message injected")

        local decoded = cjson.decode(injected[1]["data"])
        local expected = {
            text = 'hello world!',
            icon_emoji = ':rocket:',
            channel = '#team',
            username = '@user',
        }
        assert.equal('KvSlack', injected[1]['payload_name'])
        assert.equal('json', injected[1]['payload_type'])
        assert.same(expected, decoded)
    end)
end)

