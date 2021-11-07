import asyncio
import functools
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from typing import Callable, Dict, Optional, Set

from aioredis import Redis

from repid.models import JobDefenition, JobResult


class RepidWorker:
    actors: Dict[str, Callable] = {}
    queues: Set[str] = set()
    __executor = ThreadPoolExecutor()

    def __init__(self, redis: Redis):
        self.__redis = redis

    def actor(self, name: Optional[str] = None, queue: str = "default"):
        # actually it is a decorator fabric, real decorator is below
        def decorator(fn):
            nonlocal self
            actor_id_pattern = queue + ":" + (name or fn.__name__)
            self.actors[actor_id_pattern] = fn
            self.queues.add(queue)

        return decorator

    async def __call__(self):
        loop = asyncio.get_event_loop()
        while True:
            for q in self.queues:
                # get a job from the queue
                task: str = await self.__redis.lpop(q)
                if task is None or not await self.__redis.exists(task):
                    continue
                # find the job on the worker
                func: Callable = next(
                    iter([i for i in self.actors.keys() if task.startswith(i)]),
                    None,
                )
                # if there is no such job on the worker, add it back to the queue
                if func is None:
                    await self.__redis.rpushx(q, task)
                    continue
                # get job's defenition
                try:
                    defenition = JobDefenition(**json.loads(await self.__redis.get(task)))
                except Exception:
                    # TODO: logging
                    continue
                # check if scheduled job needs to be done
                if defenition.defer_until is not None:
                    if datetime.now() < defenition.defer_until:
                        # too early to run this job - add it to the end of the queue
                        await self.__redis.rpushx(q, task)
                        continue
                # check if recurring job needs to be done
                if defenition.defer_by is not None:
                    if await self.__redis.exists("result:" + task):
                        old_res = JobResult(**json.loads(await self.__redis.get("result:" + task)))
                        if (
                            old_res.finished_when is None
                            or datetime.now() < old_res.finished_when + defenition.defer_by
                        ):
                            await self.__redis.rpushx(q, task)
                            continue
                # create a result field for the job
                res = JobResult(
                    success=None,
                    started_when=datetime.now(),
                    finished_when=None,
                    result=None,
                )
                await self.__redis.set("result:" + task, json.dumps(asdict(res)))
                # run the job
                try:
                    if asyncio.iscoroutinefunction(func):
                        res.result = await func(**defenition.kwargs)
                    else:
                        res.result = await loop.run_in_executor(
                            self.__executor,
                            functools.partial(
                                func,
                                **defenition.kwargs,
                            ),
                        )
                except Exception as exc:
                    res.success = False
                    res.result = f"Exception occured while running the job: {type(exc)}: {exc}."
                    if defenition.retries > 0:
                        defenition.retries -= 1  # TODO: retries replenishment
                        await self.__redis.set(task, json.dumps(asdict(defenition)))
                        await self.__redis.lpushx(q, task)
                else:
                    res.success = True
                finally:
                    res.finished_when = datetime.now()
                    await self.__redis.set("result:" + task, json.dumps(asdict(res)), xx=True)
            await asyncio.sleep(1)
