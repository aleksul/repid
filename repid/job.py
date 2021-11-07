import json
from typing import Optional

from aioredis import Redis

from repid.models import JobResult, JobStatus


class Job:
    def __init__(self, redis: Redis, uid: str):
        self.__redis = redis
        self.uid = uid
        self.result_uid = "result:" + uid

    @property
    async def expires_in(self) -> Optional[int]:
        seconds_left = await self.__redis.ttl(self.uid)
        if seconds_left < 0:
            return None
        return seconds_left

    @property
    async def status(self) -> JobStatus:
        if not await self.__redis.exists(self.uid):
            return JobStatus.NOT_FOUND
        _result = await self.result
        if _result is None:
            return JobStatus.QUEUED
        if _result.success is None:
            return JobStatus.IN_PROGRESS
        else:
            return JobStatus.DONE

    @property
    async def result(self) -> Optional[JobResult]:
        unparsed = await self.__redis.get(self.result_uid)
        if unparsed is None:
            return None
        parsed: dict = json.loads(unparsed)
        return JobResult(**parsed)
