from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import orjson
from redis.asyncio import Redis

from .constants import JOB_PREFIX, QUEUE_PREFIX
from .job import Job
from .queue import Queue


class Repid:
    """Main class. Mostly useful for producer.
    Helps enqueueing new jobs, get existing jobs and queues, as well as pop jobs.
    """

    QUEUE_PREFIX_LEN = len(QUEUE_PREFIX)

    def __init__(self, redis: Redis):
        if redis.connection_pool.connection_kwargs.get("decode_responses") is not True:
            raise ValueError("Redis instance must decode responses.")
        self.__redis__ = redis

    async def get_all_queues(self) -> List[Queue]:
        return [
            Queue(q[self.QUEUE_PREFIX_LEN :])  # noqa: E203
            for q in await self.__redis__.keys("queue:*")
        ]

    def get_queue(self, queue_name: str) -> Queue:
        return Queue(self.__redis__, queue_name)

    async def get_job(self, id_: str) -> Optional[Job]:
        if (raw := await self.__redis__.get(JOB_PREFIX + id_)) is not None:
            return Job(self.__redis__, **orjson.loads(raw))
        return None

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

    async def _pop_normal_job(self, queue_name: str) -> Optional[Job]:
        if (job_id := await self.__redis__.lpop(QUEUE_PREFIX + queue_name)) is not None:
            if (raw_job := await self.__redis__.get(JOB_PREFIX + job_id)) is not None:
                return Job(self.__redis__, **orjson.loads(raw_job))
        return None

    async def _pop_deferred_job(self, queue_name: str) -> Optional[Job]:
        queue = Queue(self.__redis__, queue_name)
        all_jobs_ids = await queue.deferred_queue_ids
        deferred_by: List[Job] = []
        deferred_until: Dict[int, Job] = dict()
        for job_id in all_jobs_ids:
            if ((j := await self.get_job(job_id)) is None) or (not j.is_deferred):
                await queue.remove_job(job_id)
                continue
            if j.defer_until is not None:
                deferred_until[j.defer_until] = j
            if j.defer_by is not None:
                deferred_by.append(j)

        # trying to get defered_until job
        keys = list(deferred_until.keys())
        if len(keys) > 0:
            j = deferred_until[min(keys)]
            if await j.is_deferred_already:
                # return the job if it is still in queue
                if await queue.remove_job(j._id) > 0:
                    return j

        # ...otherwise, check all defered_by jobs, until find one that's ready
        while len(deferred_by) > 0:
            j = deferred_by.pop()
            if await j.is_deferred_already:
                if await queue.remove_job(j._id) > 0:
                    return j
        return None

    async def pop_job(self, queue_name: str) -> Optional[Job]:
        if (defered_job := await self._pop_deferred_job(queue_name)) is not None:
            return defered_job
        return await self._pop_normal_job(queue_name)
