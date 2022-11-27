from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Iterable

from repid.connections.abc import ConsumerT
from repid.connections.redis.utils import (
    get_priorities_order,
    mnc,
    parse_short_message_name,
    qnc,
    unix_time,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from repid.connections.redis.message_broker import RedisMessageBroker
    from repid.data.protocols import ParametersT, PrioritiesT, RoutingKeyT


class _RedisConsumer(ConsumerT):
    POLLING_WAIT: float = 0.1
    PREFETCH_AMOUNT: int = 10

    def __init__(
        self,
        broker: RedisMessageBroker,
        queue_name: str,
        topics: Iterable[str] | None = None,
        max_unacked_messages: int | None = None,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.topics = frozenset(topics) if topics is not None else frozenset()
        self.conn: Redis[bytes] = broker.conn

        self.queue: asyncio.Queue[tuple[RoutingKeyT, str, ParametersT]] = asyncio.Queue(
            maxsize=0 if max_unacked_messages is None else max_unacked_messages
        )
        self.pause_lock = asyncio.Lock()
        self.consume_task: asyncio.Task | None = None

    async def start(self) -> None:
        self.consume_task = asyncio.create_task(self.backgroud_consume())

    async def pause(self) -> None:
        if not self.pause_lock.locked():
            await self.pause_lock.acquire()

    async def unpause(self) -> None:
        if self.pause_lock.locked():
            self.pause_lock.release()

    async def finish(self) -> None:
        if self.consume_task is not None:
            self.consume_task.cancel()
        rejects = []
        while self.queue.qsize() > 0:
            key, _, _ = self.queue.get_nowait()
            rejects.append(self.broker.reject(key))
        await asyncio.gather(*rejects)

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        return await self.queue.get()

    async def backgroud_consume(self) -> None:
        while True:
            if self.pause_lock.locked():
                await self.pause_lock.acquire()
                self.pause_lock.release()
            msg = await self.consume_or_none()
            if msg is not None:
                await self.queue.put(msg)
            else:
                await asyncio.sleep(self.POLLING_WAIT)

    async def consume_or_none(self) -> tuple[RoutingKeyT, str, ParametersT] | None:
        for priority in get_priorities_order(self.broker._priorities):
            msg = await self.__get_message(priority)
            if msg is None:
                await asyncio.sleep(self.POLLING_WAIT)
                continue
            key, _, params = msg
            if params.is_overdue:
                await self.broker.nack(key)
                continue
            return msg
        return None

    async def __fetch_message_name(
        self,
        full_queue_name: str,
        startswith_topics: tuple[str, ...],
        delayed: bool,
    ) -> str | None:
        names: list[bytes] = [b""]  # pre-populate with empty bytes to meet start condition
        offset = 0
        while len(names) > 0:  # iterate until there is nothing to see in the queue
            # fetch new names from the queue
            try:
                if not delayed:
                    # fetch from the normal queue
                    names = await self.conn.lrange(
                        full_queue_name,
                        offset - self.PREFETCH_AMOUNT,  # range from the end of the queue
                        offset - 1,
                    )
                    offset -= self.PREFETCH_AMOUNT  # reversed offset
                else:
                    # fetch from delayed queue
                    names = await self.conn.zrange(  # type: ignore[call-overload]
                        full_queue_name,
                        start="-inf",  # minimum score
                        end=unix_time(),  # maximum score
                        byscore=True,
                        offset=offset,
                        num=self.PREFETCH_AMOUNT,
                    )
                    offset += self.PREFETCH_AMOUNT
            except Exception:
                return None

            # check if any of the new message names is meeting `startswith_topics` condition
            for name in names:
                str_name = name.decode()
                if not startswith_topics or str_name.startswith(startswith_topics):
                    return str_name
        return None

    async def __get_message_name(
        self,
        full_queue_name: str,
        topics: frozenset[str],
        delayed: bool = False,
    ) -> str | None:
        new_topics = tuple(map(lambda x: x + ":", topics))
        msg_short_name = await self.__fetch_message_name(full_queue_name, new_topics, delayed)
        if msg_short_name is None:
            return None
        async with self.conn.pipeline(transaction=True) as pipe:
            # remove message from the queue
            if not delayed:
                pipe.lrem(full_queue_name, -1, msg_short_name)
            else:
                pipe.zrem(full_queue_name, msg_short_name)
            # mark message as processing
            pipe.zadd(self.broker.processing_queue, {msg_short_name: str(unix_time())})
            try:
                await pipe.execute()
            except Exception:
                return None
        return msg_short_name

    async def __get_message(
        self, priority: PrioritiesT
    ) -> tuple[RoutingKeyT, str, ParametersT] | None:
        # try delayed queue first...
        msg_short_name = await self.__get_message_name(
            qnc(self.queue_name, priority, delayed=True), self.topics, delayed=True
        )
        # if there is no message in delayed queue, try normal queue
        if msg_short_name is None:
            msg_short_name = await self.__get_message_name(
                qnc(self.queue_name, priority), self.topics
            )
        # no message found - return None
        if msg_short_name is None:
            return None
        # something found - try to parse the message
        topic, id_ = parse_short_message_name(msg_short_name)
        routing_key = self.broker.ROUTING_KEY_CLASS(
            id_=id_,
            topic=topic,
            queue=self.queue_name,
            priority=priority.value,
        )

        payload: bytes | None = await self.conn.hget(mnc(routing_key), "payload")
        parameters: bytes | None = await self.conn.hget(mnc(routing_key), "parameters")

        if payload is None or parameters is None:
            # message's data was removed (but somehow id was present in the queue :shrug:)
            # - put it to the dead queue
            async with self.conn.pipeline(transaction=True) as pipe:
                # remove from the processing queue
                pipe.zrem(self.broker.processing_queue, msg_short_name)
                # put to the dead queue
                pipe.lpush(qnc(self.queue_name, dead=True), msg_short_name)
                await pipe.execute()
            # retry
            return await self.__get_message(priority)

        return (
            routing_key,
            payload.decode(),
            self.broker.PARAMETERS_CLASS.decode(parameters.decode()),
        )
