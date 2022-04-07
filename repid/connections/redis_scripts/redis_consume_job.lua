local high_priority_queue = KEYS[1]  -- list
local deferred_queue = KEYS[2]  -- sorted set
local normal_priority_queue = KEYS[3]  -- list
local low_priority_queue = KEYS[4]  -- list
local processing_queue = KEYS[5]  -- set

local current_time = ARGV[1]  -- int (unix timestamp)

local function mark_processing(id)
    redis.call('sadd', processing_queue, id)
end

local job_id

if redis.call('exists', high_priority_queue) == 1 then
    job_id = redis.call('rpop', high_priority_queue)
    mark_processing(job_id)
    return job_id
end

if redis.call('exists', deferred_queue) == 1 then
    job_id = next(redis.call('zrange', deferred_queue, '-inf', current_time, 'BYSCORE', 'LIMIT', 0, 1))
    if job_id ~= nil then
        redis.call('zrem', deferred_queue, job_id)
        mark_processing(job_id)
        return job_id
    end
end

if redis.call('exists', normal_priority_queue) == 1 then
    job_id = redis.call('rpop', normal_priority_queue)
    mark_processing(job_id)
    return job_id
end

if redis.call('exists', low_priority_queue) == 1 then
    job_id = redis.call('rpop', low_priority_queue)
    mark_processing(job_id)
    return job_id
end

return {}
