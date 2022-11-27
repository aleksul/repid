from __future__ import annotations

import logging
from asyncio import create_task, sleep
from random import randint
from typing import TYPE_CHECKING

from redis.asyncio.client import Pipeline, Redis

from repid.middlewares import InjectMiddleware
from repid.serializer import MessageSerializer
from repid.utils import unix_time

from .utils import get_priorities_order, mnc, parse_priorities_distribution, qnc

if TYPE_CHECKING:
    from repid.data import Message, PrioritiesT

logger = logging.getLogger(__name__)


@InjectMiddleware
class RedisMessaging:

    supports_delayed_messages = True
    priorities_distribution = "10/3/1"
    processing_queue = "processing"  # inside of redis is a sorted set
    run_maintenance_every: int = 600  # seconds
    prefetch_amount = 10

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = Redis.from_url(dsn)
        self._priorities = parse_priorities_distribution(self.priorities_distribution)

        if self.run_maintenance_every > 0:

            async def run_maintenance() -> None:
                await self.maintenance()
                await sleep(self.run_maintenance_every)
                await run_maintenance()

            async def first_run_maintenance() -> None:
                await sleep(randint(0, self.run_maintenance_every))  # noqa: S311
                await run_maintenance()

            create_task(first_run_maintenance())

    async def __fetch_message_name(
        self,
        full_queue_name: str,
        delayed: bool,
        startswith_topics: tuple[str, ...],
        pipe: Pipeline,
    ) -> str | None:
        names: list[bytes] = [b""]
        offset = 0
        while len(names) > 0:
            for name in names:
                str_name = name.decode()
                if str_name.startswith(startswith_topics):
                    return str_name
            try:
                if not delayed:
                    names = await pipe.immediate_execute_command(
                        "LRANGE",
                        full_queue_name,
                        offset - self.prefetch_amount,
                        offset - 1,
                    )
                    offset -= self.prefetch_amount
                else:
                    names = await pipe.immediate_execute_command(
                        "ZRANGE",
                        full_queue_name,
                        "-inf",  # minimum score
                        unix_time(),  # maximum score
                        "BYSCORE",
                        "LIMIT",
                        offset,  # offset
                        self.prefetch_amount,  # count
                    )
                    offset += self.prefetch_amount
            except Exception:
                return None
        return None

    async def __get_message_name(
        self,
        full_queue_name: str,
        topics: frozenset[str],
        delayed: bool = False,
    ) -> str | None:
        startswith_topics = tuple(map(lambda x: x + ":", topics))
        async with self.conn.pipeline(transaction=True) as pipe:
            await pipe.watch(full_queue_name)
            msg_short_name = await self.__fetch_message_name(
                full_queue_name, delayed, startswith_topics, pipe
            )
            if msg_short_name is not None:
                pipe.multi()
                # remove message from the queue
                if not delayed:
                    pipe.lrem(full_queue_name, -1, msg_short_name)
                else:
                    pipe.zrem(full_queue_name, msg_short_name)
                pipe.zadd(  # mark message as processing
                    self.processing_queue, {msg_short_name: str(unix_time())}
                )
                await pipe.execute()
        return msg_short_name

    async def __get_message(
        self,
        queue_name: str,
        priority: PrioritiesT,
        topics: frozenset[str],
    ) -> Message | None:
        # try delayed queue first...
        msg_short_name = await self.__get_message_name(
            qnc(queue_name, priority, delayed=True), topics, delayed=True
        )
        # if there is no message in delayed queue, try normal queue
        if msg_short_name is None:
            msg_short_name = await self.__get_message_name(qnc(queue_name, priority), topics)
        # no message found - return None
        if msg_short_name is None:
            return None
        # something found - try to parse the message
        msg_data = await self.conn.get(f"m:{queue_name}:{msg_short_name}")
        if msg_data is None:
            # message's data was removed (but somehow id was present in the queue :shrug:)
            # - put it to the dead queue
            async with self.conn.pipeline(transaction=True) as pipe:
                pipe.zrem(self.processing_queue, msg_short_name)  # remove from the processing queue
                pipe.lpush(qnc(queue_name, dead=True), msg_short_name)  # put to the dead queue
            return await self.__get_message(queue_name, priority, topics)
        return MessageSerializer.decode(msg_data)

    def __put_in_queue(self, msg: Message, pipe: Pipeline, in_front: bool = False) -> None:
        if not msg.is_deferred:
            if not in_front:
                pipe.lpush(qnc(msg.queue, msg.priority), mnc(msg, short=True))
            else:
                pipe.rpush(qnc(msg.queue, msg.priority), mnc(msg, short=True))
        else:
            pipe.zadd(
                qnc(msg.queue, msg.priority, delayed=True),
                {mnc(msg, short=True): str(msg.delay_until)},
            )

    def __mark_dead(self, msg: Message, pipe: Pipeline) -> None:
        pipe.lpush(qnc(msg.queue, dead=True), mnc(msg, short=True))

    def __unmark_processing(self, msg: Message, pipe: Pipeline) -> None:
        pipe.zrem(self.processing_queue, mnc(msg, short=True))

    async def consume(self, queue_name: str, topics: frozenset[str]) -> Message:
        logger.debug(f"Consuming from {queue_name = }; {topics = }.")
        while True:
            for priority in get_priorities_order(self._priorities):
                message = await self.__get_message(queue_name, priority, topics)
                if message is None:
                    continue
                if message.is_overdue:
                    await self.nack(message)
                    continue
                return message

    async def enqueue(self, message: Message) -> None:
        logger.debug(f"Enqueuing {message = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), MessageSerializer.encode(message), nx=True)
            self.__put_in_queue(message, pipe)
            await pipe.execute()

    async def ack(self, message: Message) -> None:
        logger.debug(f"Acking {message = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(message))
            self.__unmark_processing(message, pipe)
            await pipe.execute()

    async def nack(self, message: Message) -> None:
        logger.debug(f"Nacking {message = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            self.__mark_dead(message, pipe)
            self.__unmark_processing(message, pipe)
            await pipe.execute()

    async def reject(self, message: Message) -> None:
        logger.debug(f"Rejecting {message = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            self.__put_in_queue(message, pipe, in_front=True)
            self.__unmark_processing(message, pipe)
            await pipe.execute()

    async def requeue(self, message: Message) -> None:
        logger.debug(f"Requeueing {message = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.set(mnc(message), MessageSerializer.encode(message), xx=True)
            self.__put_in_queue(message, pipe, in_front=True)
            self.__unmark_processing(message, pipe)
            await pipe.execute()

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug(f"Declaring queue {queue_name = }.")
        return

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug(f"Flushing queue {queue_name = }.")
        async with self.conn.pipeline(transaction=True) as pipe:
            async for msg in self.conn.scan_iter(match=f"m:{queue_name}:*"):
                pipe.delete(msg)
            async for queue in self.conn.scan_iter(match=f"q:{queue_name}:*"):
                pipe.delete(queue)
            await pipe.execute()

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug(f"Deleting queue {queue_name = }.")
        await self.queue_flush(queue_name)

    async def maintenance(self) -> None:
        logger.info("Running maintenance.")
        now = unix_time()
        async for id_, processing_start_time in self.conn.zscan_iter(self.processing_queue):
            async for msg_short_name in self.conn.scan_iter(match=f"m:*:{id_}"):
                msg = await self.conn.get(msg_short_name)
                if msg is None:
                    await self.conn.zrem(self.processing_queue, id_)
                    continue
                message = MessageSerializer.decode(msg)
                if now - processing_start_time > message.execution_timeout:
                    logger.warning(f"Message {message.id_} timed out. Rescheduling.")
                    async with self.conn.pipeline(transaction=True) as pipe:
                        self.__mark_dead(message, pipe)
                        self.__unmark_processing(message, pipe)
                        await pipe.execute()
                    logger.info(f"Message {message.id_} was rescheduled.")
        logger.info("Maintenance done.")
