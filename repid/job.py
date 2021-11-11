import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional

from aioredis import Redis


class JobStatus(Enum):
    QUEUED = 1
    IN_PROGRESS = 2
    DONE = 3
    NOT_FOUND = 4


@dataclass
class JobResult:
    success: bool
    started_when: datetime
    finished_when: datetime
    result: Any


class Job:
    _id: str
    name: str
    queue: str
    func_args: Dict[str, Any]
    defer_until: Optional[datetime]
    defer_by: Optional[timedelta]

    def __init__(
        self,
        redis: Redis,
        name: str,
        queue: str = "default",
        func_args: Optional[Dict[str, Any]] = None,
        defer_until: Optional[datetime] = None,
        defer_by: Optional[timedelta] = None,
        _id: Optional[str] = None,
    ):
        self.name = name
        self.queue = queue
        self.func_argumnets = func_args or dict()
        self._id = _id or f"{self.name}:{uuid.uuid4().hex}"
        if bool(defer_until and defer_by):
            raise ValueError("Usage of 'defer_until' AND 'defer_by' together is prohibited.")
        self.defer_until = defer_until
        self.defer_by = defer_by
        # in redis
        self.__redis = redis
        self.__id_redis = "job:" + self._id
        self.__result_redis = "result:" + self._id
        self.__queue_redis = "queue:" + self.queue

    async def enqueue(self, expires_in: Optional[int] = 86400):
        if await self.__redis.lpos(self.__queue_redis, self.__id_redis) is None:
            await self.__redis.set(
                self.__id_redis,
                json.dumps(self.__dict__()),
                ex=expires_in,
                nx=True,
            )
            await self.__redis.rpush(
                self.__queue_redis,
                self.__id_redis,
            )

    @property
    async def expires_in(self) -> Optional[int]:
        seconds_left = await self.__redis.ttl(self.__id_redis)
        if seconds_left < 0:
            return None
        return seconds_left

    @property
    async def status(self) -> JobStatus:
        if not await self.__redis.exists(self.__id_redis):
            return JobStatus.NOT_FOUND
        if await self.__redis.lpos(self.__queue_redis, self.__id_redis) is not None:
            return JobStatus.QUEUED
        if await self.result is not None:
            return JobStatus.DONE
        else:
            return JobStatus.IN_PROGRESS

    @property
    async def result(self) -> Optional[JobResult]:
        _result = await self.__redis.get(self.__result_redis)
        return None if _result is None else JobResult(**json.loads(_result))

    def is_scheduled_now(self) -> bool:
        if (self.defer_until is None) or (self.defer_until > datetime.now()):
            return False
        return True

    async def is_reccuring_now(self) -> bool:
        if self.defer_by is None:
            return False
        previous_result = await self.result
        if previous_result is not None:
            if previous_result.finished_when + self.defer_by < datetime.now():
                return False
        return True

    def __dict__(self):
        return dict(
            _id=self._id,
            name=self.name,
            queue=self.queue,
            func_args=self.func_args,
            defer_until=self.defer_until,
            defer_by=self.defer_by,
        )
