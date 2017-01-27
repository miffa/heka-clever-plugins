-- Allow importing from parent directory
package.path = package.path .. ";../?.lua"

local mocks = require 'mocks'
local util = require 'util'

describe("Clever Container fields", function()
    it("Log lines must have programname", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), -1, "process_message should fail")
        written_messages = mocks.written_messages()
    end)
    it("Container fields are created from programname", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("production--some-api/12345678-1234-1234-1234-1234567890ab", written_messages["Fields[logtag]"])
        assert.equals("production", written_messages["Fields[container_env]"])
        assert.equals("some-api", written_messages["Fields[container_app]"])
        assert.equals("12345678-1234-1234-1234-1234567890ab", written_messages["Fields[container_task]"])
    end)
    it("Container field container_env can be overriden", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mock_msg.Fields.container_env = "development"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("development--some-api/12345678-1234-1234-1234-1234567890ab", written_messages["Fields[logtag]"])
        assert.equals("development", written_messages["Fields[container_env]"])
        assert.equals("some-api", written_messages["Fields[container_app]"])
        assert.equals("12345678-1234-1234-1234-1234567890ab", written_messages["Fields[container_task]"])
    end)
    it("Container field container_app can be overriden", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mock_msg.Fields.container_app = "apiservice"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("production--apiservice/12345678-1234-1234-1234-1234567890ab", written_messages["Fields[logtag]"])
        assert.equals("production", written_messages["Fields[container_env]"])
        assert.equals("apiservice", written_messages["Fields[container_app]"])
        assert.equals("12345678-1234-1234-1234-1234567890ab", written_messages["Fields[container_task]"])
    end)
    it("Container field container_task can be overriden", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mock_msg.Fields.container_task = "abcd"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("production--some-api/abcd", written_messages["Fields[logtag]"])
        assert.equals("production", written_messages["Fields[container_env]"])
        assert.equals("some-api", written_messages["Fields[container_app]"])
        assert.equals("abcd", written_messages["Fields[container_task]"])
    end)
    it("All Container fields can be overriden", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mock_msg.Fields.container_env = "development"
        mock_msg.Fields.container_app = "apiservice"
        mock_msg.Fields.container_task = "abcd"
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("development--apiservice/abcd", written_messages["Fields[logtag]"])
        assert.equals("development", written_messages["Fields[container_env]"])
        assert.equals("apiservice", written_messages["Fields[container_app]"])
        assert.equals("abcd", written_messages["Fields[container_task]"])
    end)
    it("Does not override container values with empty values", function()
        mocks.reset()
        require 'clever_container_decoder'

        local mock_msg= {}
        mock_msg['Timestamp'] = 2000000
        mock_msg['Hostname'] = "hostname"
        mock_msg.Fields = {}
        mock_msg.Fields.programname = "production--some-api/arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F12345678-1234-1234-1234-1234567890ab"
        mock_msg.Fields.container_env = ""
        mock_msg.Fields.container_app = ""
        mock_msg.Fields.container_task = ""
        mocks.set_next_message(mock_msg)

        assert.equals(process_message(), 0, "Should process_message successfully")
        written_messages = mocks.written_messages()

        assert.equals("production--some-api/12345678-1234-1234-1234-1234567890ab", written_messages["Fields[logtag]"])
        assert.equals("production", written_messages["Fields[container_env]"])
        assert.equals("some-api", written_messages["Fields[container_app]"])
        assert.equals("12345678-1234-1234-1234-1234567890ab", written_messages["Fields[container_task]"])
    end)
end)
