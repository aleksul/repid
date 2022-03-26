from typing import List

from redis.asyncio import Redis

from .constants import JOB_PREFIX, QUEUE_DEFER_PREFIX, QUEUE_PREFIX, VALID_NAME_RE


class Queue:
    """Adds CRUD-like operations with jobs' ids in queue.
    Those ids can be transformed into proper `Job` classes.
    Handles both normal and deferred queues, acts like one.
    """

    __slots__ = ("name", "__redis__")

    def __init__(self, redis: Redis, name: str = "default") -> None:
        self.__redis__ = redis
        if not VALID_NAME_RE.fullmatch(name):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        self.name = name

    @property
    async def ids(self) -> List[str]:
        normal_queue = await self.__redis__.lrange(QUEUE_PREFIX + self.name, 0, -1)
        defer_queue = await self.__redis__.lrange(QUEUE_DEFER_PREFIX + self.name, 0, -1)
        return normal_queue + defer_queue

    @property
    async def normal_queue_ids(self) -> List[str]:
        return await self.__redis__.lrange(QUEUE_PREFIX + self.name, 0, -1)

    @property
    async def deferred_queue_ids(self) -> List[str]:
        return await self.__redis__.lrange(QUEUE_DEFER_PREFIX + self.name, 0, -1)

    async def clean(self) -> None:
        for id_ in await self.ids:
            if not await self.__redis__.exists(JOB_PREFIX + id_):
                await self.__redis__.lrem(QUEUE_PREFIX + self.name, count=0, value=id_)
                await self.__redis__.lrem(QUEUE_DEFER_PREFIX + self.name, count=0, value=id_)

    async def clear(self) -> None:
        await self.__redis__.delete(
            QUEUE_PREFIX + self.name,
            QUEUE_DEFER_PREFIX + self.name,
        )

    async def add_job(self, job_id: str, deferred: bool = False) -> None:
        prefix = QUEUE_DEFER_PREFIX if deferred else QUEUE_PREFIX
        full_name = prefix + self.name
        async with self.__redis__.pipeline() as pipe:
            pipe.lrem(full_name, 0, job_id)
            pipe.rpush(full_name, job_id)
            await pipe.execute()

    async def remove_job(self, job_id: str) -> int:
        count1 = await self.__redis__.lrem(QUEUE_PREFIX + self.name, count=0, value=job_id)
        count2 = await self.__redis__.lrem(QUEUE_DEFER_PREFIX + self.name, count=0, value=job_id)
        return count1 + count2

    async def is_job_queued(self, job_id: str) -> bool:
        res1 = await self.__redis__.lpos(QUEUE_PREFIX + self.name, job_id)
        res2 = await self.__redis__.lpos(QUEUE_DEFER_PREFIX + self.name, job_id)
        return any([res1 is not None, res2 is not None])

    def __eq__(self, other):
        if isinstance(other, Queue):
            return self.__hash__() == other.__hash__()
        return False

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return f"Queue({self.name})"
