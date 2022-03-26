import asyncio
from typing import Callable, Dict, Optional

import orjson
from redis.asyncio import Redis

from .actor import Actor
from .constants import RESULT_PREFIX
from .job import Job, JobResult
from .queue import Queue
from .repid import Repid


class Worker(Repid):
    """Extends main class functionality to add ability to run jobs on consumers."""

    actors: Dict[str, Actor] = dict()

    def __init__(self, redis: Redis):
        self.__redis__ = redis
        super().__init__(redis)

    def actor(self, name: Optional[str] = None, queue: str = "default", retries=1):
        # actually it is a decorator fabric

        def decorator(fn):
            a = Actor(fn, name, queue, retries)
            self.actors[a.name] = a
            return fn

        return decorator

    def _get_func(self, job: Job) -> Optional[Callable]:
        return self.actors.get(job.name)

    async def _save_result(self, job_id: str, result: JobResult) -> None:
        await self.__redis__.set(RESULT_PREFIX + job_id, orjson.dumps(result))

    async def run_one_queue(self, queue: Queue):
        # pop a job from the queue
        if (job := await super().pop_job(queue.name)) is None:
            return
        # find the job on the worker
        # if there is no such job on the worker, add it back to the queue
        if (func := self._get_func(job)) is None:
            await job.queue.add_job(job._id, job.is_deferred)  # re-enqueue job
            return
        # run the job
        result: JobResult = await func(**job.func_args)
        # re-enqueue defer_by job
        if job.defer_by is not None:
            await job.queue.add_job(job._id, deferred=True)
        await self._save_result(job._id, result)

    async def run(self):
        queues = {Queue(self.__redis__, q.queue) for q in list(self.actors.values())}
        await asyncio.gather(*[self.run_one_queue(q) for q in queues])

    async def run_forever(self, delay: float = 1.0):
        while 1:  # pragma: no cover
            await self.run()
            await asyncio.sleep(delay)
