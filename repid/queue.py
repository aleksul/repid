from typing import List, Optional

import orjson
from aioredis import Redis

from repid import JOB_PREFIX, QUEUE_DEFER_PREFIX, QUEUE_PREFIX
from repid.job import Job


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

    async def remove_job(self, job_id: str) -> None:
        await self.__redis__.lrem(QUEUE_PREFIX + self.name, count=0, value=job_id)
        await self.__redis__.lrem(QUEUE_DEFER_PREFIX + self.name, count=0, value=job_id)

    async def is_job_queued(self, job_id: str) -> bool:
        res1: Optional[int] = await self.__redis__.lpos(QUEUE_PREFIX + self.name, job_id)
        res2: Optional[int] = await self.__redis__.lpos(QUEUE_DEFER_PREFIX + self.name, job_id)
        return any([res1, res2])

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
