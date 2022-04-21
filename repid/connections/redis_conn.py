import random
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator, List, Optional

from redis.asyncio import Redis

from repid.data import DeferredMessage, Message, Serializer
from repid.middlewares.middleware import with_middleware
from repid.utils import VALID_PRIORITIES, PrioritiesT, current_unix_time
from repid.utils import message_name_constructor as mnc
from repid.utils import queue_name_constructor as qnc

if TYPE_CHECKING:
    from repid.data import AnyMessageT
    from repid.queue import Queue

ProcessingQueue = "processing"  # sorted set


class RedisMessaging:
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

    async def __reschedule_deferred_by(self, message: DeferredMessage) -> None:
        if message.defer_by is None:
            return
        queue = qnc(message.queue, message.priority, delayed=True)
        message.delay_until = message.timestamp + (
            message.defer_by * ((current_unix_time() - message.timestamp) // message.defer_by + 1)
        )
        async with self.connection.pipeline(transaction=True) as pipe:
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
                    await self.__reschedule_deferred_by(message)
                return message  # type: ignore[return-value]
        return None

    @with_middleware
    async def enqueue(self, message: AnyMessageT) -> None:
        async with self.connection.pipeline() as pipe:
            pipe.set(
                mnc(message.queue, message.id_),
                Serializer.encode(message),
                exat=message.timestamp + message.ttl if message.ttl is not None else None,
            )
            if type(message) is Message:
                queue = qnc(message.queue, message.priority)
                pipe.lpush(queue, message.id_)
            elif type(message) is DeferredMessage:
                queue = qnc(message.queue, message.priority, delayed=True)
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
                        id_=f"retry-{current_unix_time()}-{message.id_}",
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
                queue = qnc(
                    message.queue,
                    message.priority,
                    delayed=True if type(message) is DeferredMessage else False,
                    dead=True,
                )
                pipe.lpush(queue, message.id_)
            await pipe.execute()

    @with_middleware
    async def message_requeue(self, message: AnyMessageT) -> None:
        if await self.connection.exists(mnc(message.queue, message.id_)):
            async with self.connection.pipeline(transaction=True) as pipe:
                pipe.lrem(
                    qnc(
                        message.queue,
                        message.priority,
                        delayed=True if type(message) is DeferredMessage else False,
                        dead=True,
                    ),
                    0,
                    message.id_,
                )
                if type(message) is Message:
                    queue = qnc(message.queue, message.priority)
                    pipe.lpush(queue, message.id_)
                elif type(message) is DeferredMessage:
                    queue = qnc(message.queue, message.priority, delayed=True)
                    pipe.zadd(queue, {message.delay_until: message.id_})
                await pipe.execute()

    async def __get_proccessing_message(self) -> AsyncGenerator:
        for offset in count(0):
            score, id_ = await self.connection.zrange(
                ProcessingQueue, offset, offset, withscores=True
            )
            if score is None or id_ is None:
                raise StopAsyncIteration
            yield (score, id_)

    @with_middleware
    async def maintenance(self):
        now = current_unix_time()
        async for score, id_ in self.__get_proccessing_message():
            k = await self.connection.keys(f"m:*:{id_}")
            if k:
                message = await self.connection.get(k)
            if message is None:
                await self.connection.zrem(ProcessingQueue, id_)
                continue
            message = Serializer.decode(message)
            if now - score > message.actor_timeout:
                await self.message_nack(message)


class RedisBucketing:
    pass
