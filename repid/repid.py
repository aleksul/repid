from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from aioredis import Redis

from .constants import QUEUE_PREFIX
from .job import Job
from .queue import Queue


class Repid:
    QUEUE_PREFIX_LEN = len(QUEUE_PREFIX)

    def __init__(self, redis: Redis):
        self.__redis__ = redis

    async def get_all_queues(self) -> List[Queue]:
        return [
            Queue(q[self.QUEUE_PREFIX_LEN :])  # noqa: E203
            for q in await self.__redis__.keys("queue:*")
        ]

    def get_queue(self, queue_name: str) -> Queue:
        return Queue(self.__redis__, queue_name)

    async def enqueue_job(
        self,
        name: str,
        queue: Union[str, Queue] = "default",
        func_args: Optional[Dict[str, Any]] = None,
        defer_until: Union[datetime, int, None] = None,
        defer_by: Union[timedelta, int, None] = None,
        _id: Optional[str] = None,
    ) -> Job:
        job = Job(
            self.__redis__,
            name,
            queue=queue,
            func_args=func_args,
            defer_until=defer_until,
            defer_by=defer_by,
            _id=_id,
        )
        await job.enqueue()
        return job
