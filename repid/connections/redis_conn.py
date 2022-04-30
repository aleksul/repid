from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

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
from repid.utils import VALID_PRIORITIES, PrioritiesT, get_priorities_order
from repid.utils import message_name_constructor as mnc
from repid.utils import next_exec_time, parse_priorities_distribution
from repid.utils import queue_name_constructor as qnc
from repid.utils import unix_time

ProcessingQueue = "processing"  # sorted set


class RedisMessaging:
    supports_delayed_messages = True
    queue_type = "FIFO"
    priorities_distribution = "10/3/1"

    def __init__(self, connection: str):
        self.conn = Redis.from_url(connection)
        scripts_path = Path(__file__).parent / "redis_scripts"
        self.consume_script = self.conn.register_script(
            script=(scripts_path / "consume.lua").read_text()
        )
        self._priorities = parse_priorities_distribution(self.__class__.priorities_distribution)

    async def __reschedule_deferred_by(
        self, message: Union[DeferredByMessage, DeferredCronMessage]
    ) -> None:
        message.delay_until = next_exec_time(message)
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(
                mnc(message.queue, message.id_),
                Serializer.encode(message),
                xx=True,
            )
            pipe.zadd(
                qnc(message.queue, message.priority, delayed=True),
                {message.delay_until: message.id_},
            )
            await pipe.execute()

    async def __is_ttl_expired(self, message: AnyMessageT) -> bool:
        if message.ttl is not None and message.timestamp + message.ttl < unix_time():
            # ttl expired - put message to the dead queue
            dead_queue = qnc(
                message.queue,
                message.priority,
                delayed=True if type(message) is DeferredMessage else False,
                dead=True,
            )
            async with self.conn.pipeline(transaction=True) as pipe:
                pipe.rpush(dead_queue, message.id_)
                pipe.zrem(ProcessingQueue, message.id_)
                await pipe.execute()
            return True
        return False

    @with_middleware
    async def consume(self, queue_name: str) -> Optional[AnyMessageT]:
        for priority in get_priorities_order(self._priorities):
            data = await self.consume_script(
                keys=(qnc(queue_name, priority), qnc(queue_name, priority, delayed=True)),
                args=(unix_time(), mnc(queue_name, "")),
            )
            if data is not None:
                message: AnyMessageT = Serializer.decode(data)  # type: ignore[assignment]
                if await self.__is_ttl_expired(message):
                    return await self.consume(queue_name)
                if isinstance(message, (DeferredByMessage, DeferredCronMessage)):
                    await self.__reschedule_deferred_by(message)
                return message
        return None

    @with_middleware
    async def enqueue(self, message: AnyMessageT) -> None:
        res = await self.conn.set(
            mnc(message.queue, message.id_),
            Serializer.encode(message),
            nx=True,
        )
        if res is None:  # message already exists
            return None
        if type(message) is Message:
            queue = qnc(message.queue, message.priority)
            await self.conn.lpush(queue, message.id_)
        elif type(message) is DeferredMessage:
            queue = qnc(message.queue, message.priority, delayed=True)
            await self.conn.zadd(queue, {message.delay_until: message.id_})

    @with_middleware
    async def queue_declare(self, queue_name: str) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue_name: str) -> None:
        async for msg in self.conn.scan_iter(match=mnc(queue_name, "*")):
            await self.conn.delete(msg)
        async for queue in self.conn.scan_iter(match=f"q:{queue_name}:*"):
            await self.conn.delete(queue)

    @with_middleware
    async def queue_delete(self, queue_name: str) -> None:
        await self.queue_flush(queue_name)

    @with_middleware
    async def ack(self, message: AnyMessageT):
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(message.queue, message.id_))
            pipe.zrem(ProcessingQueue, message.id_)
            await pipe.execute()

    @with_middleware
    async def nack(self, message: AnyMessageT):
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.zrem(ProcessingQueue, message.id_)
            if message.retries_left > 1:
                message.retries_left -= 1
                if isinstance(message, (DeferredByMessage, DeferredCronMessage)):
                    message = Message(
                        id_=f"retry-{message.id_}-{unix_time()}",
                        queue=message.queue,
                        actor_name=message.actor_name,
                        retries_left=message.retries_left,
                        actor_timeout=message.actor_timeout,
                        timestamp=message.timestamp,
                        ttl=message.ttl,
                    )
                pipe.set(
                    mnc(message.queue, message.id_),
                    Serializer.encode(message),
                    nx=True,
                )
                pipe.lpush(qnc(message.queue, message.priority), message.id_)
            else:
                queue = qnc(
                    message.queue,
                    message.priority,
                    delayed=True if type(message) is DeferredMessage else False,
                    dead=True,
                )
                pipe.lpush(queue, message.id_)
            await pipe.execute()

    @with_middleware
    async def requeue(
        self,
        message: AnyMessageT,
        unmark_processing: bool = False,
        unmark_dead: bool = True,
    ) -> None:
        msg_name = mnc(message.queue, message.id_)
        if await self.conn.exists(msg_name):
            async with self.conn.pipeline(transaction=True) as pipe:
                pipe.set(msg_name, Serializer.encode(message), xx=True)
                if type(message) is Message:
                    queue = qnc(message.queue, message.priority)
                    pipe.lpush(queue, message.id_)
                else:
                    queue = qnc(message.queue, message.priority, delayed=True)
                    pipe.zadd(queue, {message.delay_until: message.id_})  # type: ignore[union-attr]
                if unmark_processing:
                    pipe.zrem(ProcessingQueue, message.id_)
                if unmark_dead:
                    pipe.lrem(f"{queue}:dead", message.id_)
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
