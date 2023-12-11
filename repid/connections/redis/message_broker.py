from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

from redis.asyncio.client import Pipeline, Redis

from repid.connections.abc import MessageBrokerT
from repid.connections.redis import utils
from repid.logger import logger

from .consumer import _RedisConsumer
from .utils import mnc, parse_priorities_distribution, qnc

if TYPE_CHECKING:
    from repid.data.protocols import ParametersT, RoutingKeyT


class RedisMessageBroker(MessageBrokerT):
    CONSUMER_CLASS = _RedisConsumer

    supports_delayed_messages = True
    priorities_distribution = "10/3/1"
    processing_queue = "processing"  # inside of redis is a sorted set

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn: Redis[bytes] = Redis.from_url(dsn)
        self._priorities = parse_priorities_distribution(self.priorities_distribution)

    async def connect(self) -> None:
        await self.conn.ping()
        await self.maintenance()

    async def disconnect(self) -> None:
        await self.maintenance()
        await self.conn.aclose(close_connection_pool=True)  # type: ignore[attr-defined]

    def __put_in_queue(
        self,
        key: RoutingKeyT,
        pipe: Pipeline,
        delay_until: int | None = None,
        *,
        in_front: bool = False,
    ) -> None:
        if delay_until is None:
            if not in_front:
                pipe.lpush(qnc(key.queue, key.priority), mnc(key, short=True))
            else:
                pipe.rpush(qnc(key.queue, key.priority), mnc(key, short=True))
        else:
            pipe.zadd(
                qnc(key.queue, key.priority, delayed=True),
                {mnc(key, short=True): str(delay_until)},
            )

    def __mark_dead(self, key: RoutingKeyT, pipe: Pipeline) -> None:
        pipe.lpush(qnc(key.queue, key.priority, dead=True), mnc(key, short=True))

    def __unmark_processing(self, key: RoutingKeyT, pipe: Pipeline) -> None:
        pipe.zrem(self.processing_queue, mnc(key, short=True))
        pipe.hdel(mnc(key), "_reject_to")

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message ({routing_key}).", extra={"routing_key": key})
        if params is None:  # pragma: no cover
            params = self.PARAMETERS_CLASS()
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.hsetnx(
                mnc(key),
                "payload",
                payload,
            )
            pipe.hsetnx(
                mnc(key),
                "parameters",
                params.encode(),
            )
            self.__put_in_queue(key, pipe, delay_until=utils.wait_timestamp(params))
            await pipe.execute()

    async def ack(self, key: RoutingKeyT) -> None:
        logger.debug("Acking message ({routing_key}).", extra={"routing_key": key})
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.delete(mnc(key))
            self.__unmark_processing(key=key, pipe=pipe)
            await pipe.execute()

    async def nack(self, key: RoutingKeyT) -> None:
        logger.debug("Nacking message ({routing_key}).", extra={"routing_key": key})
        async with self.conn.pipeline(transaction=True) as pipe:
            self.__mark_dead(key, pipe)
            self.__unmark_processing(key, pipe)
            await pipe.execute()

    async def reject(self, key: RoutingKeyT) -> None:
        logger.debug("Rejecting message ({routing_key}).", extra={"routing_key": key})

        raw_params: list[bytes | None] = await self.conn.hmget(
            mnc(key),
            keys=["parameters", "_reject_to"],
        )

        if raw_params[0] is not None:
            params = self.PARAMETERS_CLASS.decode(raw_params[0].decode())
        else:  # pragma: no cover
            params = self.PARAMETERS_CLASS()

        reject_to = "n"  # normal queue
        if raw_params[1] is not None:
            reject_to = raw_params[1].decode()

        async with self.conn.pipeline(transaction=True) as pipe:
            if reject_to == "dead":
                self.__mark_dead(key, pipe)
            else:
                self.__put_in_queue(
                    key,
                    pipe,
                    delay_until=utils.wait_timestamp(params),
                    in_front=True,
                )
            self.__unmark_processing(key, pipe)
            await pipe.execute()

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Requeueing message ({routing_key}).", extra={"routing_key": key})
        if params is None:  # pragma: no cover
            params = self.PARAMETERS_CLASS()
        async with self.conn.pipeline(transaction=True) as pipe:
            pipe.hset(mnc(key), mapping={"payload": payload, "parameters": params.encode()})
            self.__put_in_queue(
                key,
                pipe,
                delay_until=utils.wait_timestamp(params),
                in_front=True,
            )
            self.__unmark_processing(key, pipe)
            await pipe.execute()

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra={"queue_name": queue_name})

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug("Flushing queue '{queue_name}'.", extra={"queue_name": queue_name})
        async with self.conn.pipeline(transaction=True) as pipe:
            async for msg in self.conn.scan_iter(match=f"m:{queue_name}:*"):
                pipe.delete(msg)
            async for queue in self.conn.scan_iter(match=f"q:{queue_name}:*"):
                pipe.delete(queue)
            await pipe.execute()

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra={"queue_name": queue_name})
        await self.queue_flush(queue_name)

    async def maintenance(self) -> None:
        logger.info("Running maintenance.")
        now = datetime.now()
        tasks: list[asyncio.Task] = []
        async for short_name, processing_start_time in self.conn.zscan_iter(self.processing_queue):
            async for full_name in self.conn.scan_iter(match=f"m:*:{short_name.decode()}"):
                raw_params: bytes | None = await self.conn.hget(full_name, "parameters")
                if raw_params is None:  # pragma: no cover
                    # drop message with no data
                    tasks.append(
                        asyncio.create_task(self.conn.zrem(self.processing_queue, short_name)),
                    )
                    continue
                params: ParametersT = self.PARAMETERS_CLASS.decode(raw_params.decode())
                if (
                    now
                    - datetime.fromtimestamp(
                        processing_start_time,
                    )
                    > params.execution_timeout
                ):
                    logger.warning(
                        "Message '{short_name}' timed out.",
                        extra={"short_name": short_name},
                    )
                    id_, topic, queue, priority = utils.parse_message_name(full_name.decode())
                    tasks.append(
                        asyncio.create_task(
                            self.reject(
                                self.ROUTING_KEY_CLASS(
                                    id_=id_,
                                    topic=topic,
                                    queue=queue,
                                    priority=priority,
                                ),
                            ),
                        ),
                    )
        await asyncio.gather(*tasks, return_exceptions=True)  # ignore exceptions
        logger.info("Maintenance done.")
