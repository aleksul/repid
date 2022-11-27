import logging
import signal
from typing import Callable, Dict, FrozenSet, Union
from uuid import UUID, uuid4

import anyio

from repid.actor import Actor
from repid.connection import Connection
from repid.data import ArgsBucket, Message, ResultBucket
from repid.data.messages import ArgsBucketMetadata, SimpleArgsBucket
from repid.main import DEFAULT_CONNECTION
from repid.utils import unix_time

logger = logging.getLogger(__name__)


class Worker:
    __slots__ = (
        "__conn",
        "actors",
        "limiter",
        "gracefull_shutdown_time",
        "limit_processed_messages_amount",
        "_processed_messages_amount",
    )

    def __init__(
        self,
        limit: int = 16,
        gracefull_shutdown_time: float = 25.0,
        limit_processed_messages_amount: int = float("inf"),  # type: ignore[assignment]
        _connection: Union[Connection, None] = None,
    ):
        self.__conn = _connection or DEFAULT_CONNECTION.get()
        self.actors: Dict[str, Actor] = {}
        self.limiter = anyio.CapacityLimiter(limit)
        self.gracefull_shutdown_time = gracefull_shutdown_time
        self._processed_messages_amount = 0
        self.limit_processed_messages_amount = limit_processed_messages_amount

    @property
    def topics(self) -> FrozenSet[str]:
        return frozenset(self.actors.keys())

    def actor(self, name: Union[str, None] = None, queue: str = "default") -> Callable:
        """Decorator fabric."""

        def decorator(fn: Callable) -> Callable:
            a = Actor(fn, name=name, queue=queue)
            self.actors[a.name] = a
            return fn

        return decorator

    async def __get_message_args(
        self, message: Message
    ) -> Union[ArgsBucket, SimpleArgsBucket, None]:
        if message.args_bucket is None:
            return None
        elif isinstance(message.args_bucket, SimpleArgsBucket):
            return message.args_bucket
        elif isinstance(message.args_bucket, ArgsBucketMetadata):
            if self.__conn.args_bucketer is None:
                raise ConnectionError("No args bucketer provided.")
            bucket = await self.__conn.args_bucketer.get_bucket(message.args_bucket.id_)
            if bucket is None:
                logger.error(f"No bucket found for id = {message.args_bucket.id_}.")
            if isinstance(bucket, ArgsBucket):
                return bucket
        return None

    async def __set_message_result(self, result: ResultBucket) -> None:
        if self.__conn.results_bucketer is None:
            logger.error("No results bucketer provided for returned data.")
            return
        await self.__conn.results_bucketer.store_bucket(result)

    async def _process_message(self, message: Message) -> None:
        actor = self.actors[message.topic]
        args_bucket = await self.__get_message_args(message)
        args = getattr(args_bucket, "args", ())
        kwargs = getattr(args_bucket, "kwargs", {})

        actor._TIME_LIMIT.set(message.execution_timeout)
        result = await actor(*args, **kwargs)

        # rescheduling (retry)
        if not result.success and message.tried + 1 < message.retries:
            message.tried += 1
            # message.delay_until = None  # TODO: exponential backoff
            await self.__conn.messager.requeue(message)
        # rescheduling (deferred)
        elif message.defer_by is not None or message.cron is not None:
            message._prepare_reschedule()
            await self.__conn.messager.requeue(message)
        # ack
        elif result.success:
            await self.__conn.messager.ack(message)
        # nack
        else:
            await self.__conn.messager.nack(message)

        # return result
        if message.result_bucket is not None:
            result_with_metadata = dict(
                id_=message.result_bucket.id_,
                ttl=message.result_bucket.ttl,
                timestamp=unix_time(),
                **result._asdict(),
            )
            await self.__set_message_result(ResultBucket(**result_with_metadata))

    async def __process_message_with_limiter(self, message: Message, task_id: UUID) -> None:
        try:
            await self._process_message(message)
        except anyio.get_cancelled_exc_class():
            await self.__conn.messager.reject(message)
            raise
        finally:
            self.limiter.release_on_behalf_of(task_id)

    async def __listen_signal(self, scope: anyio.CancelScope) -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            await signals.__anext__()
            self.limiter.total_tokens *= -1
            await anyio.sleep(self.gracefull_shutdown_time)
            scope.cancel()

    async def run(self) -> None:
        queue_names = {q.queue for q in self.actors.values()}
        topics_by_queue = {
            q: frozenset(t for t in self.topics if self.actors[t].queue == q) for q in queue_names
        }
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.__listen_signal, tg.cancel_scope)
            while self.limit_processed_messages_amount > self._processed_messages_amount:
                for queue_name in queue_names:
                    if not self.limit_processed_messages_amount > self._processed_messages_amount:
                        break
                    task_id = uuid4()
                    await self.limiter.acquire_on_behalf_of(task_id)
                    message = await self.__conn.messager.consume(
                        queue_name, topics_by_queue[queue_name]
                    )
                    self._processed_messages_amount += 1
                    tg.start_soon(self.__process_message_with_limiter, message, task_id)
            while self.limiter.borrowed_tokens > 0:  # Let the tasks finish
                await anyio.sleep(1.0)
            tg.cancel_scope.cancel()
