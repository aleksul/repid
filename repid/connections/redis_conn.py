import random
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import orjson
from redis.asyncio import Redis  # type: ignore

from repid.data import DeferredMessage, Message, Serializer
from repid.middlewares.middleware import with_middleware
from repid.utils import VALID_PRIORITIES, PrioritiesT, current_unix_time
from repid.utils import message_name_constructor as mnc
from repid.utils import queue_name_constructor as qnc

if TYPE_CHECKING:
    from redis.asyncio.client.Redis import pipeline as Pipeline

    from repid.data import AnyMessageT
    from repid.queue import Queue

ResultPrefix = "result"
ProcessingQueue = "processing"  # sorted set


class RedisConnection:
    supports_delayed_messages = True
    queue_type = "FIFO"
    priorities_distribution = "10/3/1"

    def __init__(self, connection: Redis) -> None:
        self.connection = connection
        scripts_path = Path(__file__).parent / "redis_scripts"
        self.consume_script = connection.register_script(
            script=(scripts_path / "consume.lua").read_text()
        )
        if not VALID_PRIORITIES.fullmatch(self.__class__.priorities_distribution):
            raise ValueError(
                f"Invalid priorities distribution: {self.__class__.priorities_distribution}"
            )
        pr_dist = [int(x) for x in self.__class__.priorities_distribution.split("/")]

        if pr_dist[0] > pr_dist[1] or pr_dist[1] > pr_dist[2]:
            raise ValueError(
                f"Invalid priorities distribution: {self.__class__.priorities_distribution}"
            )

        pr_dist_sum = sum(pr_dist)
        self._priorities = [x / pr_dist_sum for x in pr_dist]

    async def __reschedule_deferred_by(
        self, message: DeferredMessage, priotrity: PrioritiesT
    ) -> None:
        if message.defer_by is None:
            return
        queue = qnc(message.queue, priotrity, delayed=True)
        message.delay_until = message.timestamp + (
            message.defer_by * ((current_unix_time() - message.timestamp) // message.defer_by + 1)
        )
        async with self.connection.pipeline() as pipe:
            pipe.set(
                mnc(message.queue, message.id_),
                Serializer.encode(message),
                exat=message.timestamp + message.ttl if message.ttl is not None else None,
            )
            pipe.zadd(queue, {message.delay_until: message.id_})
            await pipe.execute()

    def __get_order(self, rand: float) -> List[PrioritiesT]:
        if rand <= self._priorities[0]:
            return [PrioritiesT.HIGH, PrioritiesT.MEDIUM, PrioritiesT.LOW]
        elif rand <= self._priorities[0] + self._priorities[1]:
            return [PrioritiesT.MEDIUM, PrioritiesT.HIGH, PrioritiesT.LOW]
        else:
            return [PrioritiesT.LOW, PrioritiesT.HIGH, PrioritiesT.MEDIUM]

    @with_middleware
    async def consume(self, queue: Queue) -> Optional[AnyMessageT]:
        for priority in self.__get_order(random.random()):
            data = await self.consume_script(
                keys=(qnc(queue.name, priority), qnc(queue.name, priority, delayed=True)),
                args=(current_unix_time(), mnc(queue.name, "")),
            )
            if data is not None:
                message = Serializer.decode(data)
                if type(message) is DeferredMessage:
                    await self.__reschedule_deferred_by(message, priority)
                return message  # type: ignore
        return None

    @with_middleware
    async def enqueue(
        self,
        message: AnyMessageT,
        priority: PrioritiesT = PrioritiesT.MEDIUM,
    ) -> None:
        async with self.connection.pipeline() as pipe:
            pipe.set(
                mnc(message.queue, message.id_),
                Serializer.encode(message),
                exat=message.timestamp + message.ttl if message.ttl is not None else None,
            )
            if type(message) is Message:
                queue = qnc(message.queue, priority)
                pipe.lpush(queue, message.id_)
            elif type(message) is DeferredMessage:
                queue = qnc(message.queue, priority, delayed=True)
                pipe.zadd(queue, {message.delay_until: message.id_})
            await pipe.execute()

    @with_middleware
    async def queue_declare(self, queue: Queue) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue: Queue) -> None:
        keys = await self.connection.keys(mnc(queue.name, "*"))
        async with self.connection.pipeline(transaction=True) as pipe:
            for key in keys:
                pipe.delete(key)
            for priority in PrioritiesT:
                pipe.delete(qnc(queue.name, priority))
                pipe.delete(qnc(queue.name, priority, delayed=True))
            await pipe.execute()

    @with_middleware
    async def queue_delete(self, queue: Queue) -> None:
        keys = await self.connection.keys(mnc(queue.name, "*"))
        async with self.connection.pipeline(transaction=True) as pipe:
            for key in keys:
                pipe.delete(key)
            for priority in PrioritiesT:
                pipe.delete(qnc(queue.name, priority))
                pipe.delete(qnc(queue.name, priority, delayed=True))
            await pipe.execute()

    @with_middleware
    async def message_ack(self, message: AnyMessageT):
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(message.queue, message.id_))
            pipe.zrem(ProcessingQueue, message.id_)
            await pipe.execute()

    @with_middleware
    async def message_nack(self, message: AnyMessageT):
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.zrem(ProcessingQueue, message.id_)
            if message.retries_left > 1:
                message.retries_left -= 1
                if type(message) is DeferredMessage and message.defer_by is not None:
                    message = Message(
                        id_=f"retry-{message.id_}",
                        queue=message.queue,
                        actor_name=message.actor_name,
                        retries_left=message.retries_left,
                        actor_timeout=message.actor_timeout,
                        bucket_id=message.bucket_id,
                        timestamp=message.timestamp,
                        ttl=message.ttl,
                    )
                pipe.set(
                    mnc(message.queue, message.id_),
                    Serializer.encode(message),
                    exat=message.timestamp + message.ttl if message.ttl is not None else None,
                )
                pipe.lpush(qnc(message.queue, message.priority), message.id_)
            else:
                queue = QueuePrefix["DEAD"] + message.queue
                pipe.lpush(queue, message.id_)
            await pipe.execute()


class Temp:
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
            pipe.delete(MessagePrefix + job_id).delete(ResultPrefix + job_id)
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
