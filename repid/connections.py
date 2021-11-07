import json
from dataclasses import asdict
from typing import Optional

from aioredis import Redis

from repid.job import Job
from repid.models import JobDefenition


class Repid:
    def __init__(self, redis: Redis):
        self.__redis = redis

    async def enqueue(self, job: JobDefenition, expires_in: Optional[int] = 86400) -> Job:
        """Enques a job.

        Args:
            job (JobDefenition): All job parameters.
            expires_in (Optional[int]): Auto-destroy timer. Set to None to disable.
            Defaults to 86400.

        Returns:
            Job: `Job` class.
        """
        d = asdict(job)
        uid = d["queue"] + ":" + d["_id"]
        if self.__redis.lpos(d["queue"], uid) is not None:
            await self.__redis.set(uid, json.dumps(d), ex=expires_in, nx=True)
            await self.__redis.rpush(d["queue"], uid)
        return Job(self.__redis, uid)
