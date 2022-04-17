local queue_name = ARGV[1]

local high_priority_queue = 'queue_high_priority:' .. queue_name  -- list
local deferred_queue = 'queue_deferred:' .. queue_name  -- sorted set
local normal_priority_queue = 'queue_normal_priority:' .. queue_name  -- list
local low_priority_queue = 'queue_low_priority:' .. queue_name  -- list
local processing_queue = 'processing'  -- set

local current_time = ARGV[2]  -- int (unix timestamp)

local function mark_processing(message_id)
    redis.call('sadd', processing_queue, message_id)
end

local msg_id

if redis.call('exists', high_priority_queue) == 1 then
    msg_id = redis.call('rpop', high_priority_queue)
    mark_processing(msg_id)
    return msg_id
end

if redis.call('exists', deferred_queue) == 1 then
    msg_id = next(redis.call('zrange', deferred_queue, '-inf', current_time, 'BYSCORE', 'LIMIT', 0, 1))
    if msg_id ~= nil then
        redis.call('zrem', deferred_queue, msg_id)
        mark_processing(msg_id)
        return msg_id
    end
end

if redis.call('exists', normal_priority_queue) == 1 then
    msg_id = redis.call('rpop', normal_priority_queue)
    mark_processing(msg_id)
    return msg_id
end

if redis.call('exists', low_priority_queue) == 1 then
    msg_id = redis.call('rpop', low_priority_queue)
    mark_processing(msg_id)
    return msg_id
end

return {}
