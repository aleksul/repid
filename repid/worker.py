import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Set

import orjson
from aioredis import Redis

from .constants import RESULT_PREFIX
from .job import Job, JobResult
from .queue import Queue
from .repid import Repid


class Worker(Repid):
    actors: Dict[str, Callable] = {}
    _queues: Set[Queue] = set()

    def __init__(self, redis: Redis):
        self.__redis__ = redis
        super().__init__(redis)

    def actor(self, name: Optional[str] = None, queue: str = "default", retries=1):
        # actually it is a decorator fabric, real decorator is below
        def decorator(fn):
            self._queues.add(Queue(self.__redis__, queue))
            actor_name = name or fn.__name__

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                result: Any = None
                success: bool = None
                started_when = int(datetime.utcnow().timestamp())
                loop = asyncio.get_event_loop()
                for _ in range(retries):
                    try:
                        if asyncio.iscoroutinefunction(fn):
                            result = await fn(*args, **kwargs)
                        else:
                            with ThreadPoolExecutor() as pool:
                                result = await loop.run_in_executor(
                                    pool,
                                    functools.partial(fn, *args, **kwargs),
                                )
                    except Exception as exc:
                        result = f"Exception occured while running the job: {type(exc)}: {exc}."
                        success = False
                    else:
                        success = True
                        break
                return JobResult(
                    success=success,
                    started_when=started_when,
                    finished_when=int(datetime.utcnow().timestamp()),
                    result=result,
                )

            self.actors[actor_name] = wrapper

            return wrapper

        return decorator

    def _get_func(self, job: Job) -> Optional[Callable]:
        return self.actors.get(job.name)

    async def _save_result(self, job_id: str, result: JobResult) -> None:
        await self.__redis__.set(RESULT_PREFIX + job_id, orjson.dumps(result))

    async def run_one_queue(self, queue: Queue):
        # pop a job from the queue
        if (job := await queue.pop_job()) is None:
            return
        # find the job on the worker
        # if there is no such job on the worker, add it back to the queue
        if (func := self._get_func(job)) is None:
            await job.queue.add_job(job._id, job.is_defered)  # re-enqueue job
            return
        # run the job
        result: JobResult = await func(**job.func_args)
        await self._save_result(job._id, result)

    async def run(self):
        await asyncio.gather(*[self.run_one_queue(queue) for queue in self._queues])

    async def run_forever(self, delay: float = 1.0):
        while 1:  # pragma: no cover
            await self.run()
            await asyncio.sleep(delay)
