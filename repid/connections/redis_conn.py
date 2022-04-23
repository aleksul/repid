import random
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from redis.asyncio import Redis

from repid.data import DeferredMessage, Message, Serializer
from repid.middlewares.middleware import with_middleware
from repid.utils import VALID_PRIORITIES, PrioritiesT
from repid.utils import message_name_constructor as mnc
from repid.utils import queue_name_constructor as qnc
from repid.utils import unix_time

if TYPE_CHECKING:
    from repid.data import AnyBucketT, AnyMessageT

ProcessingQueue = "processing"  # sorted set


class RedisMessaging:
    supports_delayed_messages = True
    queue_type = "FIFO"
    priorities_distribution = "10/3/1"

    def __init__(self, connection: Redis):
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

    async def __reschedule_deferred_by(self, message: DeferredMessage) -> None:
        if message.defer_by is None:
            return
        message.delay_until = message.timestamp + (
            message.defer_by * ((unix_time() - message.timestamp) // message.defer_by + 1)
        )
        async with self.connection.pipeline(transaction=True) as pipe:
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
            async with self.connection.pipeline(transaction=True) as pipe:
                pipe.rpush(dead_queue, message.id_)
                pipe.zrem(ProcessingQueue, message.id_)
                await pipe.execute()
            return True
        return False

    def __get_order(self, rand: float) -> List[PrioritiesT]:
        if rand <= self._priorities[0]:
            return [PrioritiesT.HIGH, PrioritiesT.MEDIUM, PrioritiesT.LOW]
        elif rand <= self._priorities[0] + self._priorities[1]:
            return [PrioritiesT.MEDIUM, PrioritiesT.HIGH, PrioritiesT.LOW]
        else:
            return [PrioritiesT.LOW, PrioritiesT.HIGH, PrioritiesT.MEDIUM]

    @with_middleware
    async def consume(self, queue_name: str) -> Optional[AnyMessageT]:
        for priority in self.__get_order(random.random()):
            data = await self.consume_script(
                keys=(qnc(queue_name, priority), qnc(queue_name, priority, delayed=True)),
                args=(unix_time(), mnc(queue_name, "")),
            )
            if data is not None:
                message: AnyMessageT = Serializer.decode(data)  # type: ignore[assignment]
                if await self.__is_ttl_expired(message):
                    return await self.consume(queue_name)
                if type(message) is DeferredMessage:
                    await self.__reschedule_deferred_by(message)
                return message
        return None

    @with_middleware
    async def enqueue(self, message: AnyMessageT) -> None:
        res = await self.connection.set(
            mnc(message.queue, message.id_),
            Serializer.encode(message),
            nx=True,
        )
        if res is None:  # message already exists
            return None
        if type(message) is Message:
            queue = qnc(message.queue, message.priority)
            await self.connection.lpush(queue, message.id_)
        elif type(message) is DeferredMessage:
            queue = qnc(message.queue, message.priority, delayed=True)
            await self.connection.zadd(queue, {message.delay_until: message.id_})

    @with_middleware
    async def queue_declare(self, queue_name: str) -> None:
        return

    @with_middleware
    async def queue_flush(self, queue_name: str) -> None:
        async for msg in self.connection.scan_iter(match=mnc(queue_name, "*")):
            await self.connection.delete(msg)
        async for queue in self.connection.scan_iter(match=f"q:{queue_name}:*"):
            await self.connection.delete(queue)

    @with_middleware
    async def queue_delete(self, queue_name: str) -> None:
        await self.queue_flush(queue_name)

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
                        id_=f"retry-{unix_time()}-{message.id_}",
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
    async def message_requeue(
        self,
        message: AnyMessageT,
        unmark_processing: bool = False,
        unmark_dead: bool = False,
    ) -> None:
        msg_name = mnc(message.queue, message.id_)
        if await self.connection.exists(msg_name):
            async with self.connection.pipeline(transaction=True) as pipe:
                pipe.set(msg_name, Serializer.encode(message), xx=True)
                if type(message) is Message:
                    queue = qnc(message.queue, message.priority)
                    pipe.lpush(queue, message.id_)
                elif type(message) is DeferredMessage:
                    queue = qnc(message.queue, message.priority, delayed=True)
                    pipe.zadd(queue, {message.delay_until: message.id_})
                if unmark_processing:
                    pipe.zrem(ProcessingQueue, message.id_)
                if unmark_dead:
                    pipe.lrem(f"{queue}:dead", message.id_)
                await pipe.execute()

    @with_middleware
    async def maintenance(self):
        """This method is called periodically to clean up the processing queue."""
        now = unix_time()
        async for id_, score in self.connection.zscan_iter(ProcessingQueue):
            k = await self.connection.scan(match=f"m:*:{id_}", count=1)
            if k is not None:
                message = await self.connection.get(k)
            if message is None:
                await self.connection.zrem(ProcessingQueue, id_)
                continue
            message: AnyMessageT = Serializer.decode(message)
            if now - score > message.actor_timeout:
                await self.message_requeue(message, unmark_processing=True)


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
