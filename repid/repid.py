import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aioredis import Redis

from repid.job import Job


class Repid:
    def __init__(self, redis: Redis):
        self.__redis = redis

    async def _get_all_queues(self) -> List[str]:
        return [q[len("queue:") :] for q in await self.__redis.keys("queue:*")]  # noqa: E203

    async def _get_queued_jobs_ids(self, queue: str) -> List[str]:
        _queue: List[str] = await self.__redis.lrange("queue:" + queue, 0, -1)
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
        _id: Optional[str] = None,
    ) -> Job:
        job = Job(
            self.__redis,
            name,
            queue=queue,
            func_args=func_args,
            defer_until=defer_until,
            defer_by=defer_by,
            _id=_id,
        )
        await job.enqueue()
        return job
