local queue = KEYS[1]  -- list
local deferred_queue = KEYS[2]  -- sorted set
local processing_queue = 'processing'  -- sorted set

local current_time = ARGV[1]  -- int (unix timestamp)
local message_prefix = ARGV[2]

local function mark_processing(message_id)
    redis.call('zadd', processing_queue, current_time, message_id)
end

local msg_id
local msg_data

if( redis.call('exists', deferred_queue) == 1 )
then
    msg_id = next(redis.call('zrange', deferred_queue, '-inf', current_time, 'BYSCORE', 'LIMIT', 0, 1))
    while( msg_id ~= nil )  -- try again if msg_data is nil
    do
        redis.call('zrem', deferred_queue, msg_id)
        msg_data = redis.call('get', message_prefix .. msg_id)
        if( msg_data ~= nil )
        then
            mark_processing(msg_id)
            return msg_data
        end
        msg_id = next(redis.call('zrange', deferred_queue, '-inf', current_time, 'BYSCORE', 'LIMIT', 0, 1))
    end
end

while( redis.call('exists', queue) == 1 )
do
    msg_id = redis.call('rpop', queue)  -- not nil, because queue exists
    msg_data = redis.call('get', message_prefix .. msg_id)
    if( msg_data ~= nil )
    then
        mark_processing(msg_id)
        return msg_data
    end
end

return nil
