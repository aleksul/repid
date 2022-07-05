import uuid
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Optional, Union

from repid.connection import Connection
from repid.data import (
    ArgsBucket,
    DeferredByMessage,
    DeferredCronMessage,
    DeferredMessage,
    Message,
    PrioritiesT,
    ResultBucket,
)
from repid.main import DEFAULT_CONNECTION
from repid.queue import Queue
from repid.utils import VALID_ID, VALID_NAME, unix_time

if TYPE_CHECKING:
    from repid.data import AnyMessageT


class Job:
    """Describes how and when the job should be executed."""

    __slots__ = (
        "id_",
        "name",
        "queue",
        "priority",
        "deferred_until",
        "deferred_by",
        "cron",
        "retries",
        "timeout",
        "created",
        "ttl",
        "__conn",
    )

    def __init__(
        self,
        name: str,
        queue: Union[str, Queue] = "default",
        priority: PrioritiesT = PrioritiesT.MEDIUM,
        deferred_until: Union[datetime, int, None] = None,
        deferred_by: Union[timedelta, int, None] = None,
        cron: Optional[str] = None,
        retries: int = 3,
        timeout: int = 600,
        ttl: Optional[int] = None,
        id_: Optional[str] = None,
        _connection: Optional[Connection] = None,
    ):
        self.__conn = _connection or DEFAULT_CONNECTION.get()

        if self.__conn is None:
            raise ValueError("No connection provided.")

        if not self.__conn.messager.supports_delayed_messages and self.is_deferred:
            raise ValueError("Deferred jobs are not supported by this connection.")

        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Job name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        self.name = name

        if id_ is not None and not VALID_ID.fullmatch(id_):
            raise ValueError("Job id must contain only letters, numbers, dashes and underscores.")
        self.id_ = id_ or uuid.uuid4().hex

        if not isinstance(queue, Queue):
            self.queue = Queue(queue, self.__conn)
        else:
            self.queue = queue

        self.priority = priority

        if (cron is not None) and (deferred_by is not None):
            raise ValueError("Usage of 'cron' AND 'deferred_by' together is prohibited.")

        if isinstance(deferred_until, datetime):
            deferred_until = int(deferred_until.timestamp())
        self.deferred_until = deferred_until

        if isinstance(deferred_by, timedelta):
            deferred_by = int(deferred_by.seconds)
        self.deferred_by = deferred_by

        self.cron = cron

        if retries < 1:
            raise ValueError("Retries must be greater than or equal to 1.")
        self.retries = retries

        if timeout < 1:
            raise ValueError("Execution timeout must be greater than or equal to 1 second.")
        self.timeout = timeout

        if ttl is not None and ttl < 1:
            raise ValueError("TTL must be greater than or equal to 1 second.")
        self.ttl = ttl

        self.created = unix_time()

    @cached_property
    def is_deferred(self) -> bool:
        return (
            (self.deferred_until is not None)
            or (self.deferred_by is not None)
            or (self.cron is not None)
        )

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return unix_time() > self.created + self.ttl
        return False

    async def enqueue(self) -> None:
        await self.__conn.messager.enqueue(self._to_message())

    @property
    async def data(self) -> Optional[ArgsBucket]:
        if self.__conn.args_bucketer is None:
            raise ConnectionError("No args bucketer provided.")
        data = await self.__conn.args_bucketer.get_bucket(f"args:{self.id_}")
        if isinstance(data, ArgsBucket):
            return data
        return None

    @data.setter
    async def data(self, value: ArgsBucket) -> None:
        if self.__conn.args_bucketer is None:
            raise ConnectionError("No args bucketer provided.")
        await self.__conn.args_bucketer.store_bucket(value)

    @property
    async def result(self) -> Optional[ResultBucket]:
        if self.__conn.results_bucketer is None:
            raise ConnectionError("Results bucketer is not configured.")
        data = await self.__conn.results_bucketer.get_bucket(f"result:{self.id_}")
        if type(data) is ResultBucket:
            return data
        return None

    def _to_message(self) -> AnyMessageT:
        cls = Message
        if self.deferred_until is not None:
            cls = DeferredMessage
        if self.deferred_by is not None:
            cls = DeferredByMessage
        if self.cron is not None:
            cls = DeferredCronMessage
        additional_kwargs = dict(
            defer_until=self.deferred_until,
            defer_by=self.deferred_by,
            cron=self.cron,
        )
        additional_kwargs = {k: v for k, v in additional_kwargs.items() if v is not None}
        return cls(
            id_=self.id_,
            topic=self.name,
            queue=self.queue.name,
            priority=self.priority,
            retries_left=self.retries,
            # TODO bucket, result_id, result_ttl
            timeout=self.timeout,
            ttl=self.ttl,
            **additional_kwargs,
        )

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Job):
            return False
        return self.id_ == __o.id_

    def __hash__(self) -> int:
        return hash(self.id_)

    def __str__(self) -> str:
        return f"Job(id={self.id_}, name={self.name}, queue={self.queue.name})"
