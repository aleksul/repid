from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import orjson
from aioredis import Redis

from .constants import JOB_PREFIX, RESULT_PREFIX

if TYPE_CHECKING:
    from .queue import Queue

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

        from .queue import Queue

        if not isinstance(queue, Queue):
            self.queue = Queue(redis, queue)
        else:
            self.queue = queue
        self.func_args = func_args or dict()

        if defer_until is not None and defer_by is not None:
            raise ValueError("Usage of 'defer_until' AND 'defer_by' together is prohibited.")

        self.defer_until = None
        if isinstance(defer_until, datetime):
            self.defer_until = int(defer_until.timestamp())
        elif type(defer_until) is int:
            self.defer_until = defer_until

        self.defer_by = None
        if isinstance(defer_by, timedelta):
            self.defer_by = int(defer_by.seconds)
        elif type(defer_by) is int:
            self.defer_by = defer_by

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
        raw = await self.__redis__.get(RESULT_PREFIX + self._id)
        return None if raw is None else JobResult(**orjson.loads(raw))

    @property
    def is_defer_until(self) -> Optional[bool]:
        if self.defer_until is None:
            return None
        if self.defer_until < int(datetime.utcnow().timestamp()):
            return True
        return False

    @property
    async def is_defer_by(self) -> Optional[bool]:
        if self.defer_by is None:
            return None
        res = await self.result
        if res is None:
            return True
        elif res.finished_when + self.defer_by < int(datetime.utcnow().timestamp()):
            return True
        else:
            return False

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
            if s == "queue":
                q: Queue = self.__getattribute__(s)
                res[s] = q.name
                continue

            if not s.startswith("__"):
                res[s] = self.__getattribute__(s)
        return res
