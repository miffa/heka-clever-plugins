--[[
Parses the programname from ECS containers into container_env, container_app, and container_task
fields.  Also adds a field called "logtag" which concats the env, app, and task into a convenient
format.
--]]

require "string"

function process_message()
    local programname = read_message("Fields[programname]")

    local force_env = read_message("Fields[container_env]")
    local force_app = read_message("Fields[container_app]")
    local force_task = read_message("Fields[container_task]")

    if programname == nil or programname == "" then return -1 end

    pat =
        "^docker/([%-%w][%-%w]-)%-%-([%-%w][%-%w]-)/".. -- env--app
        "arn%%3Aaws%%3Aecs%%3Aus%-west%-1%%3A589690932525%%3Atask%%2F".. -- ARN cruft
        "(%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x)$" -- task-id

    _, _, env, app, task = string.find(programname, pat)

    -- override env, app, task if forced
    if force_env and force_env ~= "" then
      env = force_env
    end
    if force_app and force_app ~= "" then
      app = force_app
    end
    if force_task and force_task ~= "" then
      task = force_task
    end

    if env and app and task then
        write_message("Fields[logtag]", ("%s--%s/%s"):format(env, app, task))
        write_message("Fields[container_env]", env)
        write_message("Fields[container_app]", app)
        write_message("Fields[container_task]", task)
    end

    return 0
end
