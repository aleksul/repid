import uuid
from enum import Enum
from pathlib import Path
from typing import Optional

import orjson
from redis.asyncio import Redis  # type: ignore

from repid.connections.connection import Messaging
from repid.middlewares.middleware import with_middleware
from repid.queue import Queue
from repid.utils import current_unix_time

JobPrefix = "job:"
ResultPrefix = "result:"
ProcessingQueue = "processing"  # set


class QueuePrefix(Enum):
    HighPriority = "queue_high_priority:"  # list
    Deferred = "queue_deferred:"  # sorted set, with time of execution as key
    NormalPriority = "queue_normal_priority:"  # list
    LowPriority = "queue_low_priority:"  # list
    Dead = "queue_dead:"  # list


class RedisConnection(Messaging):
    supports_delayed_messages = True
    queue_type = "FIFO"

    def __init__(self, connection: Redis) -> None:
        self.connection = connection
        scripts_path = Path(__file__).parent / "redis_scripts"
        self.consume_job_script = connection.register_script(
            script=(scripts_path / "redis_consume_job.lua").read_text()
        )

    @with_middleware
    async def consume(self, queue: Queue) -> AsyncGenerator[AnyMessage, None]:
        
        

    @with_middleware
    async def enqueue(  # TODO: overload
        self,
        message: AnyMessage,
        priority: Literal["HIGH", "NORMAL", "LOW"] = "NORMAL",
    ) -> None:
        """Appends the message to the queue."""
        ...

    @with_middleware
    async def queue_declare(self, queue: Queue) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue: Queue) -> None:
        # NOTE: flush and delete are exactly the same in Redis
        await self.delete_queue(queue)

    @with_middleware
    async def queue_delete(self, queue: Queue) -> None:
        async with self.connection.pipeline(transaction=True) as pipe:
            for prefix in QueuePrefix:
                pipe.delete(prefix.value + queue.name)
            await pipe.execute()

    # ===============================================================================

    async def consume_job(self, queue_name: str) -> Optional[JobData]:
        job_id = await self.consume_job_script(
            keys=(
                QueuePrefix.HighPriority.value + queue_name,
                QueuePrefix.Deferred.value + queue_name,
                QueuePrefix.NormalPriority.value + queue_name,
                QueuePrefix.LowPriority.value + queue_name,
                ProcessingQueue,
            ),
            args=(current_unix_time(),),
        )
        if job_id is not None:
            return await self.read_job(job_id)
        return None

    async def enqueue_job(self, job: JobData) -> None:
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.set(
                JobPrefix + job.id_,  # type: ignore
                orjson.dumps(job),
                exat=(job.created + job.ttl) if job.ttl is not None else None,
            )
            if job.dead:
                pipe.lpush(QueuePrefix.Dead.value + job.queue, job.id_)
            elif job.next_exec_time is not None:
                pipe.zadd(QueuePrefix.Deferred.value + job.queue, {job.next_exec_time: job.id_})
            elif job.priority == "DEFERRED":
                pipe.zadd(QueuePrefix.Deferred.value + job.queue, {job.deferred_until: job.id_})
            elif job.priority == "HIGH":
                pipe.lpush(QueuePrefix.HighPriority.value + job.queue, job.id_)
            elif job.priority == "NORMAL":
                pipe.lpush(QueuePrefix.NormalPriority.value + job.queue, job.id_)
            elif job.priority == "LOW":
                pipe.lpush(QueuePrefix.LowPriority.value + job.queue, job.id_)
            await pipe.execute()

    async def read_job(self, job_id: str) -> Optional[JobData]:
        job_data = await self.connection.get(JobPrefix + job_id)
        if job_data is not None:
            return orjson.loads(job_data)
        return None

    async def report_job(self, result: JobResult) -> None:
        async with self.connection.pipeline(transaction=True) as pipe:
            await (
                pipe.srem(ProcessingQueue, result.id_)
                .set(
                    ResultPrefix + result.id_,
                    orjson.dumps(result),
                    exat=(result.created + result.ttl) if result.ttl is not None else None,
                )
                .execute()
            )

        if (job := await self.read_job(result.id_)) is None:
            return None
        now = current_unix_time()
        job.updated = now

        # requeue deferred_by jobs regardless of result
        if job.deferred_by is not None:
            # calculate new next_exec_time
            job.next_exec_time = job.created + (
                job.deferred_by * ((now - job.created) // job.deferred_by + 1)
            )
            await self.enqueue_job(job)

        if not result.success:
            if job.retries_left > 1:
                job.retries_left -= 1
                backoff = 10 ** (
                    job.retries - job.retries_left if job.retries - job.retries_left <= 5 else 5
                )  # limited to 1 day
                job.next_exec_time = now + backoff
                # create a separate retry job for deferred_by jobs
                if job.deferred_by is not None:
                    job.deferred_by = None
                    job.id_ = f"{job.id_}:retry:{uuid.uuid4().hex}"
            else:
                job.retries_left = 0
                job.next_exec_time = None
                job.dead = True
            await self.enqueue_job(job)

    async def read_result(self, job_id: str) -> Optional[JobResult]:
        result_data = await self.connection.get(ResultPrefix + job_id)
        if result_data is not None:
            return orjson.loads(result_data)
        return None

    async def requeue_job(self, job_id: str) -> None:
        if (job := await self.read_job(job_id)) is not None:
            job.dead = False
            job.retries_left = job.retries
            now = current_unix_time()
            job.updated = now
            job.next_exec_time = None
            if job.deferred_by is not None:
                job.next_exec_time = job.created + (
                    job.deferred_by * ((now - job.created) // job.deferred_by + 1)
                )
            await self.enqueue_job(job)

    async def delete_job(self, job_id: str, delete_from_queues: bool = True) -> None:
        job = await self.read_job(job_id)
        if job is None:
            return None
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.delete(JobPrefix + job_id).delete(ResultPrefix + job_id)
            if delete_from_queues:
                if job.priority == "HIGH":
                    pipe.lrem(name=QueuePrefix.HighPriority.value + job.queue, value=job_id)
                elif job.priority == "NORMAL":
                    pipe.lrem(name=QueuePrefix.NormalPriority.value + job.queue, value=job_id)
                elif job.priority == "LOW":
                    pipe.lrem(name=QueuePrefix.LowPriority.value + job.queue, value=job_id)
                elif job.priority == "DEFERRED":
                    pipe.zrem(name=QueuePrefix.Deferred.value + job.queue, value=job_id)
            await pipe.execute()
