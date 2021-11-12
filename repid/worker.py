import asyncio
import functools
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Set

from aioredis import Redis

from repid.job import Job, JobResult
from repid.repid import Repid


class Worker(Repid):
    actors: Dict[str, Callable] = {}
    queues: Set[str] = set()
    __executor = ThreadPoolExecutor()

    def __init__(self, redis: Redis):
        self.__redis = redis
        super().__init__(redis)

    def actor(self, name: Optional[str] = None, queue: str = "default", retries=1):
        # actually it is a decorator fabric, real decorator is below
        def decorator(fn):
            self.queues.add(queue)
            actor_name = name or fn.__name__
            self.actors[actor_name] = fn

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                loop = asyncio.get_event_loop()
                result: Any = None
                success: bool = None
                started_when = datetime.now()
                for _ in range(retries):
                    try:
                        if asyncio.iscoroutinefunction(fn):
                            result = await fn(*args, **kwargs)
                        else:
                            result = await loop.run_in_executor(
                                self.__executor,
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
                    finished_when=datetime.now(),
                    result=result,
                )

            return wrapper

        return decorator

    async def _pop_job(self, queue: str) -> Optional[Job]:
        if not await self.__redis.exists(queue):
            return None
        job_id = await self.__redis.lpop(queue)
        if job_id is None:
            return None
        raw_job = await self.__redis.get(job_id)
        if raw_job is None:
            return None
        job = Job(self.__redis, **json.loads(raw_job))
        return job

    def _get_func(self, job: Job) -> Optional[Callable]:
        return self.actors.get(job.name)

    async def _get_func_or_reschedule_job(self, job: Job) -> Optional[Callable]:
        func = self._get_func(job)
        if func is None:
            await job.enqueue()
        return func

    async def run(self):
        for queue in self.queues:
            # get a job from the queue
            job = await self._pop_job(queue)
            if job is None:
                continue
            # find the job on the worker
            # if there is no such job on the worker, add it back to the queue
            func = await self._get_func_or_reschedule_job(job)
            if func is None:
                continue
            # check if scheduled or reccuring job needs to be done
            if job.is_scheduled_now() and await job.is_reccuring_now():
                # run the job
                result: JobResult = await func(**job.func_args)
                await self.__redis.set(job._result_redis, json.dumps(asdict(result)))
        asyncio.ensure_future(self.run)
