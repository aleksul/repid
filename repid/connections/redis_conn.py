import uuid
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator, Literal, Optional

import msgspec
import orjson
from redis.asyncio import Redis  # type: ignore

from repid.message import DeferredMessage, Message
from repid.middlewares.middleware import with_middleware
from repid.utils import current_unix_time

if TYPE_CHECKING:
    from redis.asyncio.client.Redis import pipeline as Pipeline

    from repid.message import AnyMessage
    from repid.queue import Queue

MessagePrefix = "job:"
ResultPrefix = "result:"
ProcessingQueue = "processing"  # set
Priorities = Literal["HIGH", "NORMAL", "LOW"]


QueuePrefix = dict(
    HIGH="queue_high_priority:",  # list
    NORMAL="queue_normal_priority:",  # list
    LOW="queue_low_priority:",  # list
    DEFERRED="queue_deferred:",  # sorted set, with time of execution as key
    DEAD="queue_dead:",  # list
)

msg_dec = msgspec.msgpack.Decoder(type=AnyMessage)  # type: ignore
msg_enc = msgspec.msgpack.Encoder()


class RedisConnection:
    supports_delayed_messages = True
    queue_type = "FIFO"

    def __init__(self, connection: Redis) -> None:
        self.connection = connection
        scripts_path = Path(__file__).parent / "redis_scripts"
        self.consume_script = connection.register_script(
            script=(scripts_path / "consume.lua").read_text()
        )

    @staticmethod
    async def _create_message(self, message: AnyMessage, pipe: Pipeline) -> None:
        pipe.set(MessagePrefix + message.id_, message.serialize())

    async def _read_message(self, id_: str) -> Optional[AnyMessage]:
        data = await self.connection.get(MessagePrefix + id_)
        if data is None:
            return None
        return self.msg_dec.decode(data)

    @with_middleware
    async def consume(self, queue: Queue) -> AsyncGenerator[AnyMessage, None]:
        while (
            msg := await self.consume_script(keys=(queue.name,), args=(current_unix_time(),))
        ) is not None:
            message: AnyMessage = self.msg_dec.decode(msg)
            if message.timestamp + message.ttl < current_unix_time():
                continue
            await self.connection.sadd(ProcessingQueue, msg)
            yield message

    @with_middleware
    async def enqueue(self, message: AnyMessage, priority: Priorities = "NORMAL") -> None:
        packed = msgspec.msgpack.encode(message)
        queue: str
        if type(message) is Message:
            queue = QueuePrefix[priority] + message.queue
            await self.connection.lpush(queue, packed)
        elif type(message) is DeferredMessage:
            queue = QueuePrefix["DEFERRED"] + message.queue
            await self.connection.zadd(queue, {message.delay_until: packed})

    @with_middleware
    async def queue_declare(self, queue: Queue) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue: Queue) -> None:
        # NOTE: flush and delete are exactly the same in Redis
        async with self.connection.pipeline(transaction=True) as pipe:
            for prefix in QueuePrefix.values():
                pipe.delete(prefix + queue.name)
            await pipe.execute()

    @with_middleware
    async def queue_delete(self, queue: Queue) -> None:
        # NOTE: flush and delete are exactly the same in Redis
        async with self.connection.pipeline(transaction=True) as pipe:
            for prefix in QueuePrefix.values():
                pipe.delete(prefix + queue.name)
            await pipe.execute()

    def __reschedule_deferred_by(self, message: DeferredMessage, pipe: Pipeline) -> None:
        if message.defer_by is None:
            return
        queue = QueuePrefix.Deferred.value + message.queue
        message.delay_until = message.timestamp + (
            message.defer_by * ((current_unix_time() - message.timestamp) // message.defer_by + 1)
        )
        # TODO
        pipe.zadd(queue, {message.delay_until: message.id_})

    @with_middleware
    async def message_ack(self, message: AnyMessage):
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.srem(ProcessingQueue, message.id_)
            if type(message) is DeferredMessage:
                self.__reschedule_deferred_by(message, pipe)
            await pipe.execute()

    @with_middleware
    async def message_nack(self, message: AnyMessage):
        pass


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
