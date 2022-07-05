import logging
import signal
from typing import TYPE_CHECKING, Callable, Dict, Optional
from uuid import uuid4

import anyio

from repid.actor import Actor
from repid.data import AnyMessageT, ArgsBucket, ResultBucket
from repid.main import DEFAULT_CONNECTION

if TYPE_CHECKING:
    from uuid import UUID

    from repid.connection import Connection


class Worker:
    __slots__ = ("__conn", "actors", "limiter", "gracefull_shutdown_time")

    def __init__(
        self,
        limit: int = 16,
        gracefull_shutdown_time: float = 25.0,
        _connection: Optional[Connection] = None,
    ):
        self.__conn = _connection or DEFAULT_CONNECTION.get()
        if self.__conn is None:
            raise ValueError("No connection provided.")

        self.actors: Dict[str, Actor] = {}
        self.limiter = anyio.CapacityLimiter(limit)
        self.gracefull_shutdown_time = gracefull_shutdown_time

    def actor(self, name: Optional[str] = None, queue: str = "default") -> Callable:
        """Decorator fabric."""

        def decorator(fn: Callable) -> Callable:
            a = Actor(fn, name=name, queue=queue)
            self.actors[a.name] = a
            return fn

        return decorator

    async def __get_message_args(self, message: AnyMessageT) -> Optional[ArgsBucket]:
        if message.bucket is None:
            return None
        elif isinstance(message.bucket, ArgsBucket):
            return message.bucket
        elif isinstance(message.bucket, str):
            if self.__conn.args_bucketer is None:
                raise ConnectionError("No args bucketer provided.")
            bucket = await self.__conn.args_bucketer.get_bucket(message.bucket)
            if bucket is None:
                logging.error(f"No bucket found for id = {message.bucket}.")
            if isinstance(bucket, ArgsBucket):
                return bucket
        return None

    async def __set_message_result(self, result: ResultBucket) -> None:
        if self.__conn.results_bucketer is None:
            logging.error("No results bucketer provided for returned data.")
            return
        await self.__conn.results_bucketer.store_bucket(result)

    async def _process_message(self, message: AnyMessageT) -> None:
        actor = self.actors[message.topic]
        args_bucket = await self.__get_message_args(message)
        if args_bucket is not None:
            args = args_bucket.args or ()
            kwargs = args_bucket.kwargs or {}
            result = await actor(*args, **kwargs)
        else:
            result = await actor()
        if result.success:
            await self.__conn.messager.ack(message)
        else:
            await self.__conn.messager.nack(message)
        if message.result_id:
            result.id_ = message.result_id
            result.ttl = message.result_ttl
            await self.__set_message_result(result)

    async def __process_message_with_limiter(self, message: AnyMessageT, task_id: UUID) -> None:
        try:
            await self._process_message(message)
        finally:
            self.limiter.release_on_behalf_of(task_id)

    async def __listen_signal(self, scope: anyio.CancelScope) -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            await anext(signals)
            self.limiter.total_tokens *= -1
            await anyio.sleep(self.gracefull_shutdown_time)
            scope.cancel()

    async def run(self) -> None:
        queue_names = {q.queue for q in self.actors.values()}
        topics = self.actors.keys()
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.__listen_signal, tg.cancel_scope)
            while True:
                for queue_name in queue_names:
                    task_id = uuid4()
                    await self.limiter.acquire_on_behalf_of(task_id)
                    message = await self.__conn.messager.consume(queue_name, topics)
                    if message is None:
                        self.limiter.release_on_behalf_of(task_id)
                        continue
                    tg.start_soon(self.__process_message_with_limiter, message, task_id)
