import random
import re
from typing import List, Union

from redis.asyncio.client import Redis

from repid.data import AnyBucketT, PrioritiesT
from repid.middlewares.wrapper import InjectMiddleware
from repid.serializer import BucketSerializer

# from repid.utils import unix_time
# from repdist.data import Message
# from repid.serializer import MessageSerializer
# from pathlib import Path
# from typing import Iterable, Union

ProcessingQueue = "processing"  # sorted set

VALID_PRIORITIES = re.compile(r"[0-9]+\/[0-9]+\/[0-9]+")


def get_priorities_order(priorities_distribution: List[float]) -> List[PrioritiesT]:
    rand = random.random()  # noqa: S311
    if rand <= priorities_distribution[0]:
        return [PrioritiesT.HIGH, PrioritiesT.MEDIUM, PrioritiesT.LOW]
    elif rand <= priorities_distribution[0] + priorities_distribution[1]:
        return [PrioritiesT.MEDIUM, PrioritiesT.HIGH, PrioritiesT.LOW]
    else:
        return [PrioritiesT.LOW, PrioritiesT.HIGH, PrioritiesT.MEDIUM]


def parse_priorities_distribution(priorities_distribution: str) -> List[float]:
    if not VALID_PRIORITIES.fullmatch(priorities_distribution):
        raise ValueError(f"Invalid priorities distribution: {priorities_distribution}")
    pr_dist = [int(x) for x in priorities_distribution.split("/")]
    pr_dist_sum = sum(pr_dist)
    return [x / pr_dist_sum for x in pr_dist]


"""
def qnc(  # queue name constructor
    name: str, priority: PrioritiesT = PrioritiesT.MEDIUM, delayed: bool = False, dead: bool = False
) -> str:
    if dead:
        return f"q:{name}:dead"
    return f"q:{name}:{priority.value}:{'d' if delayed else 'n'}"


def mnc(message: Message, short: bool = False) -> str:  # message name constructor
    prefix = ""
    if not short:
        prefix = f"m:{message.queue}:"
    if isinstance(message, DeferredMessage):
        return f"{prefix}{message.topic}:{message.id_}:{message.delay_until}"
    else:
        return f"{prefix}{message.topic}:{message.id_}"



@InjectMiddleware
class RedisMessaging:

    supports_delayed_messages = True
    priorities_distribution = "10/3/1"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = Redis.from_url(dsn)
        self.consume_script = self.conn.register_script(
            script=(Path(__file__).parent / "redis_scripts" / "consume.lua").read_text()
        )
        self._priorities = parse_priorities_distribution(self.__class__.priorities_distribution)

    async def __reschedule_deferred(
        self, message: Union[DeferredByMessage, DeferredCronMessage]
    ) -> None:
        message.delay_until = message.next_execution_time
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), Serializer.encode(message), nx=True)
            pipe.zadd(
                qnc(message.queue, message.priority, delayed=True),
                {str(message.delay_until): mnc(message, short=True)},
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

    async def consume(
        self, queue_name: str, topics: Optional[Iterable[str]] = None
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

    async def enqueue(self, message: AnyMessageT) -> None:
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), Serializer.encode(message), nx=True)
            if not isinstance(message, DeferredMessage):
                pipe.lpush(qnc(message.queue, message.priority), mnc(message, short=True))
            else:
                pipe.zadd(
                    qnc(message.queue, message.priority, delayed=True),
                    {str(message.delay_until): mnc(message, short=True)},
                )
            await pipe.execute()

    async def queue_declare(self, queue_name: str) -> None:
        return

    async def queue_flush(self, queue_name: str) -> None:
        async for msg in self.conn.scan_iter(match=f"m:{queue_name}:*"):
            await self.conn.delete(msg)
        async for queue in self.conn.scan_iter(match=f"q:{queue_name}:*"):
            await self.conn.delete(queue)

    async def queue_delete(self, queue_name: str) -> None:
        await self.queue_flush(queue_name)

    async def ack(self, message: AnyMessageT) -> None:
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(message))
            pipe.zrem(ProcessingQueue, mnc(message, short=True))
            await pipe.execute()

    async def nack(self, message: AnyMessageT) -> None:
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.zrem(ProcessingQueue, mnc(message, short=True))
            if message.retries_left > 1:
                message.retries_left -= 1
                pipe.set(mnc(message), Serializer.encode(message))
                if type(message) is Message:
                    pipe.lpush(qnc(message.queue, message.priority), mnc(message, short=True))
                elif isinstance(message, DeferredMessage):
                    pipe.zadd(
                        qnc(message.queue, message.priority, delayed=True),
                        {str(message.delay_until): mnc(message, short=True)},
                    )
            else:
                pipe.lpush(
                    qnc(message.queue, message.priority, dead=True),
                    mnc(message, short=True),
                )
            await pipe.execute()

    async def requeue(
        self,
        message: AnyMessageT,
        unmark_processing: bool = False,
        unmark_dead: bool = True,
    ) -> None:
        if not await self.conn.exists(mnc(message)):
            return
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), Serializer.encode(message), xx=True)
            if type(message) is Message:
                pipe.lpush(qnc(message.queue, message.priority), mnc(message, short=True))
            elif isinstance(message, DeferredMessage):
                pipe.zadd(
                    qnc(message.queue, message.priority, delayed=True),
                    {str(message.delay_until): mnc(message, short=True)},
                )
            if unmark_processing:
                pipe.zrem(ProcessingQueue, mnc(message, short=True))
            if unmark_dead:
                pipe.lrem(
                    name=qnc(message.queue, message.priority, dead=True),
                    count=0,
                    value=mnc(message, short=True),
                )
            await pipe.execute()

    async def maintenance(self) -> None:
        now = unix_time()
        async for id_, processing_start_time in self.conn.zscan_iter(ProcessingQueue):
            _, i = await self.conn.scan(match=f"m:*:{id_}", count=1)
            k: Optional[str] = i[0] if len(i) > 0 else None
            msg = None
            if k is not None:
                msg = await self.conn.get(k)
            if msg is None:
                await self.conn.zrem(ProcessingQueue, id_)
                continue
            message: AnyMessageT = Serializer.decode(msg)  # type: ignore[assignment]
            if now - processing_start_time > message.timeout:
                await self.requeue(message, unmark_processing=True)
"""


@InjectMiddleware
class RedisBucketing:
    def __init__(self, dsn: str):
        self.conn = Redis.from_url(dsn)

    async def get_bucket(self, id_: str) -> Union[AnyBucketT, None]:
        data = await self.conn.get(id_)
        if data is not None:
            return BucketSerializer.decode(data)
        return None

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        await self.conn.set(
            bucket.id_,
            BucketSerializer.encode(bucket),
            exat=bucket.timestamp + bucket.ttl if bucket.ttl is not None else None,
        )

    async def delete_bucket(self, id_: str) -> None:
        await self.conn.delete(id_)


class RedisMessaging:
    pass
