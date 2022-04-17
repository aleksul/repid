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

MessagePrefix = "message:"
ResultPrefix = "result:"
ProcessingQueue = "processing"  # set
Priorities = ("HIGH", "MEDIUM", "LOW")

PrioritiesT = Literal["HIGH", "MEDIUM", "LOW"]


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
    def _create_message(message: AnyMessage, pipe: Pipeline) -> None:
        pipe.set(
            f"{MessagePrefix}{message.queue}:{message.id_}",
            msg_enc.encode(message),
            exat=message.timestamp + message.ttl if message.ttl is not None else None,
        )

    async def _read_message(self, id_: str, queue: str) -> Optional[AnyMessage]:
        message = await self.connection.get(f"{MessagePrefix}{queue}:{id_}")
        if message is None:
            return None
        return msg_dec.decode(message)

    @staticmethod
    def _delete_message(id_: str, queue: str, pipe: Pipeline) -> None:
        pipe.delete(f"{MessagePrefix}{queue}:{id_}")

    async def __reschedule_deferred_by(self, message: DeferredMessage) -> None:
        if message.defer_by is None:
            return
        queue = QueuePrefix["DEFERRED"] + message.queue
        message.delay_until = message.timestamp + (
            message.defer_by * ((current_unix_time() - message.timestamp) // message.defer_by + 1)
        )
        async with self.connection.pipeline() as pipe:
            self._create_message(message, pipe)
            pipe.zadd(queue, {message.delay_until: message.id_})
            await pipe.execute()

    @with_middleware
    async def consume(self, queue: Queue) -> AsyncGenerator[AnyMessage, None]:
        while (
            id_ := await self.consume_script(args=(queue.name, current_unix_time()))
        ) is not None:
            message = await self._read_message(id_, queue.name)
            if message is None:
                await self.connection.srem(ProcessingQueue, id_)
                continue
            if type(message) is DeferredMessage:
                await self.__reschedule_deferred_by(message)
            yield message

    @with_middleware
    async def enqueue(self, message: AnyMessage, priority: Priorities = "NORMAL") -> None:
        async with self.connection.pipeline() as pipe:
            self._create_message(message, pipe)
            if type(message) is Message:
                queue = QueuePrefix[priority] + message.queue
                pipe.lpush(queue, message.id_)
            elif type(message) is DeferredMessage:
                queue = QueuePrefix["DEFERRED"] + message.queue
                pipe.zadd(queue, {message.delay_until: message.id_})
            await pipe.execute()

    @with_middleware
    async def queue_declare(self, queue: Queue) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue: Queue) -> None:
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.delete(f"{MessagePrefix}{queue.name}:*")
            for prefix in QueuePrefix.values():
                pipe.delete(prefix + queue.name)
            await pipe.execute()

    @with_middleware
    async def queue_delete(self, queue: Queue) -> None:
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.delete(f"{MessagePrefix}{queue.name}:*")
            for prefix in QueuePrefix.values():
                pipe.delete(prefix + queue.name)
            await pipe.execute()

    @with_middleware
    async def message_ack(self, message: AnyMessage):
        async with self.connection.pipeline(transaction=True) as pipe:
            self._delete_message(message.id_, message.queue, pipe)
            pipe.srem(ProcessingQueue, message.id_)
            await pipe.execute()

    @with_middleware
    async def message_nack(self, message: AnyMessage):
        async with self.connection.pipeline(transaction=True) as pipe:
            pipe.srem(ProcessingQueue, message.id_)
            if message.retries_left > 1:
                message.retries_left -= 1

                if type(message) is DeferredMessage and message.defer_by is not None:
                    pass
                else:
                    
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
