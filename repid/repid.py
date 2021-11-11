import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aioredis import Redis

from repid.job import Job

# Job in redis pattern: job:{uid} where uid usually {name}:{uuid4().hex}


class Repid:
    def __init__(self, redis: Redis):
        self.__redis = redis

    async def _get_all_queues(self) -> List[str]:
        return await self.__redis.keys("queue:*")

    async def _get_queued_jobs_ids(self, queue: str) -> List[str]:
        _queue: List[str] = await self.__redis.get(queue)
        if _queue is None:
            raise ValueError(f"Queue with name {queue} doesn't exist.")
        return _queue

    async def _get_queued_jobs(self, queue: str) -> List[Job]:
        _queue = await self._get_queued_jobs_ids(queue=queue)
        res = []
        for _id in _queue:
            d = await self.__redis.get(_id)
            if d is None:
                continue
            res.append(Job(self.__redis, **json.loads(d)))
        return res

    async def enqueue_job(
        self,
        name: str,
        queue: str = "default",
        func_args: Optional[Dict[str, Any]] = None,
        defer_until: Optional[datetime] = None,
        defer_by: Optional[timedelta] = None,
        expires_in: Optional[int] = 86400,
        _id: Optional[str] = None,
    ) -> Job:
        j = Job(
            self.__redis,
            name,
            queue=queue,
            func_args=func_args,
            defer_until=defer_until,
            defer_by=defer_by,
            _id=_id,
        )
        await j.enqueue(expires_in=expires_in)
        return j
