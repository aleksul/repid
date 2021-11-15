import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import orjson
from aioredis import Redis

from repid import JOB_PREFIX, RESULT_PREFIX
from repid.queue import Queue

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class JobStatus(Enum):
    QUEUED = 1
    IN_PROGRESS = 2
    DONE = 3
    NOT_FOUND = 4


@dataclass
class JobResult:
    success: bool
    started_when: int
    finished_when: int
    result: JSONType


class Job:
    __slots__ = ("_id", "name", "queue", "func_args", "defer_until", "defer_by", "__redis__")

    def __init__(
        self,
        redis: Redis,
        name: str,
        queue: Union[str, Queue] = "default",
        func_args: Optional[Dict[str, JSONType]] = None,
        defer_until: Union[datetime, int, None] = None,
        defer_by: Union[timedelta, int, None] = None,
        _id: Optional[str] = None,
    ):
        self.__redis__ = redis
        self.name = name
        self._id = _id or f"{name}:{uuid.uuid4().hex}"
        if not isinstance(queue, Queue):
            self.queue = Queue(redis, queue)
        else:
            self.queue = queue
        self.func_args = func_args or dict()

        if defer_until is not None and defer_by is not None:
            raise ValueError("Usage of 'defer_until' AND 'defer_by' together is prohibited.")

        self.defer_until = defer_until
        if isinstance(defer_until, datetime):
            self.defer_until = int(defer_until.timestamp())

        self.defer_by = defer_by
        if isinstance(defer_until, timedelta):
            self.defer_by = int(defer_by.seconds)

    async def enqueue(self):
        await self.__redis__.set(
            JOB_PREFIX + self._id,
            orjson.dumps(self.__as_dict__()),
            nx=True,
        )
        await self.queue.add_job(self._id, self.is_defered)

    @property
    def is_defered(self) -> bool:
        return (self.defer_until is not None) or (self.defer_by is not None)

    @property
    async def status(self) -> JobStatus:
        if not await self.__redis__.exists(JOB_PREFIX + self._id):
            return JobStatus.NOT_FOUND
        if await self.queue.is_job_queued(self._id):
            return JobStatus.QUEUED
        if await self.result is not None:
            return JobStatus.DONE
        else:
            return JobStatus.IN_PROGRESS  # pragma: no cover

    @property
    async def result(self) -> Optional[JobResult]:
        _result = await self.__redis__.get(RESULT_PREFIX + self._id)
        return None if _result is None else JobResult(**orjson.loads(_result))

    @property
    def is_defer_until(self) -> bool:
        if self.defer_until is None:
            return True
        if self.defer_until > datetime.now().timestamp():  # type: ignore
            return False
        return True

    @property
    async def is_defer_by(self) -> bool:
        if self.defer_by is None:
            return True
        res = await self.result
        if res is not None:
            if res.finished_when + self.defer_by < datetime.now().timestamp():  # type: ignore
                return False
        return True

    def __eq__(self, other):
        if isinstance(other, Job):
            return all(
                [
                    self._id == other._id,
                    self.name == other.name,
                    self.queue == other.queue,
                    self.func_args == other.func_args,
                    self.defer_until == other.defer_until,
                    self.defer_by == other.defer_by,
                ]
            )
        return False

    def __as_dict__(self) -> Dict[str, Any]:
        res = dict()
        for s in self.__slots__:
            if not s.startswith("__"):
                res[s] = self.__getattribute__(s)
        return res
