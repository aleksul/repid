local queue = KEYS[1]  -- list
local deferred_queue = KEYS[2]  -- sorted set
local processing_queue = 'processing'  -- sorted set

local current_time = ARGV[2]  -- int (unix timestamp)

local function mark_processing(message_id)
    redis.call('sadd', processing_queue, current_time, message_id)
end

local msg_id

if redis.call('exists', deferred_queue) == 1 then
    msg_id = next(redis.call('zrange', deferred_queue, '-inf', current_time, 'BYSCORE', 'LIMIT', 0, 1))
    if msg_id ~= nil then
        redis.call('zrem', deferred_queue, msg_id)
        mark_processing(msg_id)
        return msg_id
    end
end

if redis.call('exists', queue) == 1 then
    msg_id = redis.call('rpop', queue)  -- not nil, because queue exists
    mark_processing(msg_id)
    return msg_id
end

return {}
