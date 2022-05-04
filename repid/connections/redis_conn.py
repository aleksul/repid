from pathlib import Path
from typing import List, Optional, Union

from redis.asyncio import Redis

from repid.data import (
    AnyBucketT,
    AnyMessageT,
    DeferredByMessage,
    DeferredCronMessage,
    DeferredMessage,
    Message,
    Serializer,
)
from repid.middlewares.middleware import with_middleware
from repid.utils import (
    PrioritiesT,
    get_priorities_order,
    next_exec_time,
    parse_priorities_distribution,
    unix_time,
)

ProcessingQueue = "processing"  # sorted set


def qnc(  # queue name constructor
    name: str, priority: PrioritiesT = PrioritiesT.MEDIUM, delayed: bool = False, dead: bool = False
) -> str:
    if dead:
        return f"q:{name}:dead"
    return f"q:{name}:{priority.value}:{'d' if delayed else 'n'}"


def mnc(message: AnyMessageT, short: bool = False) -> str:  # message name constructor
    prefix = ""
    if not short:
        prefix = f"m:{message.queue}:"
    if isinstance(message, DeferredMessage):
        return f"{prefix}{message.actor_name}:{message.id_}:{message.delay_until}"
    else:
        return f"{prefix}{message.actor_name}:{message.id_}"


class RedisMessaging:
    supports_delayed_messages = True
    queue_type = "FIFO"
    priorities_distribution = "10/3/1"

    def __init__(self, connection: str):
        self.conn = Redis.from_url(connection)
        self.consume_script = self.conn.register_script(
            script=(Path(__file__).parent / "redis_scripts" / "consume.lua").read_text()
        )
        self._priorities = parse_priorities_distribution(self.__class__.priorities_distribution)

    async def __reschedule_deferred(self, message: Union[DeferredByMessage, DeferredCronMessage]):
        message.delay_until = next_exec_time(message)
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), Serializer.encode(message), nx=True)
            pipe.zadd(
                qnc(message.queue, message.priority, delayed=True),
                {message.delay_until: mnc(message, short=True)},
            )
            await pipe.execute()

    async def __is_ttl_expired(self, message: AnyMessageT) -> bool:
        if message.ttl is not None and message.timestamp + message.ttl < unix_time():
            # ttl expired - put message to the dead queue
            msg_name = mnc(message, short=True)
            async with self.conn.pipeline(transaction=True) as pipe:
                pipe.rpush(qnc(message.queue, message.priority, dead=True), msg_name)
                pipe.zrem(ProcessingQueue, msg_name)
                await pipe.execute()
            return True
        return False

    @with_middleware
    async def consume(
        self, queue_name: str, topics: Optional[List[str]] = None
    ) -> Optional[AnyMessageT]:
        for priority in get_priorities_order(self._priorities):
            # TODO: topics
            data = await self.consume_script(
                keys=(qnc(queue_name, priority), qnc(queue_name, priority, delayed=True)),
                args=(unix_time(), f"m:{queue_name}:"),
            )
            if data is not None:
                message: AnyMessageT = Serializer.decode(data)  # type: ignore[assignment]
                if await self.__is_ttl_expired(message):
                    return await self.consume(queue_name)
                if isinstance(message, (DeferredByMessage, DeferredCronMessage)):
                    await self.__reschedule_deferred(message)
                return message
        return None

    @with_middleware
    async def enqueue(self, message: AnyMessageT) -> None:
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), Serializer.encode(message), nx=True)
            if type(message) is Message:
                pipe.lpush(qnc(message.queue, message.priority), mnc(message, short=True))
            elif isinstance(message, DeferredMessage):
                pipe.zadd(
                    qnc(message.queue, message.priority, delayed=True),
                    {message.delay_until: mnc(message, short=True)},
                )
            await pipe.execute()

    @with_middleware
    async def queue_declare(self, queue_name: str) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue_name: str) -> None:
        async for msg in self.conn.scan_iter(match=f"m:{queue_name}:*"):
            await self.conn.delete(msg)
        async for queue in self.conn.scan_iter(match=f"q:{queue_name}:*"):
            await self.conn.delete(queue)

    @with_middleware
    async def queue_delete(self, queue_name: str) -> None:
        await self.queue_flush(queue_name)

    @with_middleware
    async def ack(self, message: AnyMessageT):
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(message))
            pipe.zrem(ProcessingQueue, mnc(message, short=True))
            await pipe.execute()

    @with_middleware
    async def nack(self, message: AnyMessageT):
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.zrem(ProcessingQueue, mnc(message, short=True))
            if message.retries_left > 1:
                message.retries_left -= 1
                # TODO: requeue
            else:
                pipe.lpush(
                    qnc(message.queue, message.priority, dead=True),
                    mnc(message, short=True),
                )
            await pipe.execute()

    @with_middleware
    async def requeue(
        self,
        message: AnyMessageT,
        unmark_processing: bool = False,
        unmark_dead: bool = True,
    ) -> None:
        msg_name = mnc(message, short=True)
        if await self.conn.exists(msg_name):
            async with self.conn.pipeline(transaction=True) as pipe:
                pipe.set(msg_name, Serializer.encode(message), xx=True)
                if type(message) is Message:
                    pipe.lpush(qnc(message.queue, message.priority), msg_name)
                elif isinstance(message, DeferredMessage):
                    pipe.zadd(
                        qnc(message.queue, message.priority, delayed=True),
                        {message.delay_until: msg_name},
                    )
                if unmark_processing:
                    pipe.zrem(ProcessingQueue, msg_name)
                if unmark_dead:
                    pipe.lrem(qnc(message.queue, message.priority, dead=True), msg_name)
                await pipe.execute()

    @with_middleware
    async def maintenance(self):
        """This method is called periodically to clean up the processing queue."""
        now = unix_time()
        async for id_, score in self.conn.zscan_iter(ProcessingQueue):
            k = await self.conn.scan(match=f"m:*:{id_}", count=1)
            msg = None
            if k is not None:
                msg = await self.conn.get(k)
            if msg is None:
                await self.conn.zrem(ProcessingQueue, id_)
                continue
            # TODO: check code
            message: AnyMessageT = Serializer.decode(msg)  # type: ignore[assignment]
            if now - score > message.actor_timeout:
                await self.requeue(message, unmark_processing=True)


class RedisBucketing:
    def __init__(self, connection: Redis):
        self.connection = connection

    @with_middleware
    async def get_bucket(self, id_: str) -> Optional[AnyBucketT]:
        data = await self.connection.get(id_)
        if data is not None:
            return Serializer.decode(data)  # type: ignore[return-value]
        return None

    @with_middleware
    async def store_bucket(self, bucket: AnyBucketT) -> None:
        await self.connection.set(
            bucket.id_,
            Serializer.encode(bucket),
            nx=True,
            exat=bucket.timestamp + bucket.ttl if bucket.ttl is not None else None,
        )

    @with_middleware
    async def delete_bucket(self, id_: str) -> None:
        await self.connection.delete(id_)
