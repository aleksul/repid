from typing import Dict, List, Optional

import orjson
from aioredis import Redis

from .constants import JOB_PREFIX, QUEUE_DEFER_PREFIX, QUEUE_PREFIX
from .job import Job


class Queue:
    __slots__ = ("name", "__redis__")

    def __init__(self, redis: Redis, name: str = "default") -> None:
        self.__redis__ = redis
        self.name = name

    @property
    async def _ids(self) -> List[str]:
        normal_queue = await self.__redis__.lrange(QUEUE_PREFIX + self.name, 0, -1)
        defer_queue = await self.__redis__.lrange(QUEUE_DEFER_PREFIX + self.name, 0, -1)
        return normal_queue + defer_queue

    async def clean(self) -> None:
        for id_ in await self._ids:
            if not await self.__redis__.exists(JOB_PREFIX + id_):
                await self.__redis__.lrem(QUEUE_PREFIX + self.name, count=0, value=id_)
                await self.__redis__.lrem(QUEUE_DEFER_PREFIX + self.name, count=0, value=id_)

    async def clear(self) -> None:
        await self.__redis__.delete(
            QUEUE_PREFIX + self.name,
            QUEUE_DEFER_PREFIX + self.name,
        )

    async def add_job(self, job_id: str, defered: bool = False) -> None:
        prefix = QUEUE_DEFER_PREFIX if defered else QUEUE_PREFIX
        full_name = prefix + self.name
        async with self.__redis__.pipeline() as pipe:
            pipe.lrem(full_name, 0, job_id)
            pipe.rpush(full_name, job_id)
            await pipe.execute()

    async def _pop_normal_job(self) -> Optional[Job]:
        job_id = await self.__redis__.lpop(QUEUE_PREFIX + self.name)
        if job_id is None:
            return None
        raw_job = await self.__redis__.get(JOB_PREFIX + job_id)
        if raw_job is None:
            return None
        return Job(self.__redis__, **orjson.loads(raw_job))

    async def _pop_defered_job(self) -> Optional[Job]:
        all_jobs_ids = await self.__redis__.lrange(QUEUE_DEFER_PREFIX + self.name, 0, -1)
        defered_by: List[Job] = []
        defered_until: Dict[int, Job] = dict()
        for job_id in all_jobs_ids:
            # get a job
            raw_job = await self.__redis__.get(JOB_PREFIX + job_id)
            if raw_job is None:
                continue
            j = Job(self.__redis__, **orjson.loads(raw_job))
            # check the job is defered
            if not j.is_defered:
                await self.remove_job(job_id)
                continue
            # add the job to appropreate list/dict
            if j.defer_until is not None:
                defered_until[j.defer_until] = j
            if j.defer_by is not None:
                defered_by.append(j)

        # trying to get defered_until job
        keys = list(defered_until.keys())
        if len(keys) > 0:
            soonest_job = defered_until[min(keys)]
            if bool(soonest_job.is_defer_until):
                # this method pops the job, but we didn't removed it yet
                await self.remove_job(soonest_job._id)
                return soonest_job

        # ...otherwise, check all defered_by jobs, until find one that's ready
        while len(defered_by) > 0:
            j = defered_by.pop()
            if bool(await j.is_defer_by):
                return j
        return None

    async def pop_job(self) -> Optional[Job]:
        defered_job = await self._pop_defered_job()
        if defered_job is not None:
            return defered_job
        return await self._pop_normal_job()

    async def remove_job(self, job_id: str) -> None:
        await self.__redis__.lrem(QUEUE_PREFIX + self.name, count=0, value=job_id)
        await self.__redis__.lrem(QUEUE_DEFER_PREFIX + self.name, count=0, value=job_id)

    async def is_job_queued(self, job_id: str) -> bool:
        res1 = await self.__redis__.lpos(QUEUE_PREFIX + self.name, job_id)
        res2 = await self.__redis__.lpos(QUEUE_DEFER_PREFIX + self.name, job_id)
        return any([res1 is not None, res2 is not None])

    @property
    async def jobs(self) -> List[Job]:
        res = []
        for id_ in await self._ids:
            raw = await self.__redis__.get(JOB_PREFIX + id_)
            if raw is None:
                continue
            j = Job(self.__redis__, **orjson.loads(raw))
            res.append(j)
        return res

    def __eq__(self, other):
        if isinstance(other, Queue):
            return self.name == other.name
        return False

    def __hash__(self):
        return hash(self.name)
