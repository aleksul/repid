import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Optional, Union

from repid import _default_connection
from repid.connections.connection import Connection
from repid.data import Bucket, DeferredMessage, Message, PrioritiesT, ResultBucket
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
        retries: int = 1,
        timeout: int = 600,
        ttl: Optional[int] = None,
        id_: Optional[str] = None,
        _connection: Optional[Connection] = None,
    ):
        self.__conn: Connection = _connection or _default_connection  # type: ignore[assignment]

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

        if (deferred_until is not None) and (deferred_by is not None):
            raise ValueError("Usage of 'deferred_until' AND 'deferred_by' together is prohibited.")

        self.deferred_until: Optional[int] = None
        if isinstance(deferred_until, datetime):
            self.deferred_until = int(deferred_until.timestamp())
        elif isinstance(deferred_until, int):
            self.deferred_until = deferred_until

        self.deferred_by: Optional[int] = None
        if isinstance(deferred_by, timedelta):
            self.deferred_by = int(deferred_by.seconds)
        elif isinstance(deferred_by, int):
            self.deferred_by = deferred_by

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
        return (self.deferred_until is not None) or (self.deferred_by is not None)

    @property
    def next_execution_time(self) -> int:
        if self.deferred_until is not None:
            return self.deferred_until
        elif self.deferred_by is not None:
            return self.created + (
                self.deferred_by * ((unix_time() - self.created) // self.deferred_by + 1)
            )
        else:
            raise TypeError("Job is not deferred.")

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return unix_time() > self.created + self.ttl
        return False

    async def enqueue(self):
        await self.__conn.messager.enqueue(self._to_message())

    @property
    async def data(self) -> Optional[Bucket]:
        if self.__conn.args_bucketer is None:
            raise ConnectionError("No args bucketer provided.")
        data = await self.__conn.args_bucketer.get_bucket(f"args:{self.id_}")
        if type(data) is Bucket:
            return data
        return None

    @data.setter
    async def data(self, value: Bucket):
        if not value.id_.startswith("args:"):
            raise ValueError("Bucket id must start with 'args:'.")
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

    @result.setter
    async def result(self, value: ResultBucket):
        if not value.id_.startswith("result:"):
            raise ValueError("Result id must start with 'result:'.")
        if self.__conn.results_bucketer is None:
            raise ConnectionError("Results bucketer is not configured.")
        await self.__conn.results_bucketer.store_bucket(value)

    def _to_message(self) -> AnyMessageT:
        if self.is_deferred:
            return DeferredMessage(
                id_=self.id_,
                actor_name=self.name,
                queue=self.queue.name,
                priority=self.priority,
                delay_until=self.next_execution_time,
                defer_by=self.deferred_by,
                retries_left=self.retries,
                actor_timeout=self.timeout,
                ttl=self.ttl,
                timestamp=self.created,
            )
        else:
            return Message(
                id_=self.id_,
                actor_name=self.name,
                queue=self.queue.name,
                priority=self.priority,
                retries_left=self.retries,
                actor_timeout=self.timeout,
                ttl=self.ttl,
                timestamp=self.created,
            )

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Job):
            return False
        return self.id_ == __o.id_

    def __hash__(self) -> int:
        return hash(self.id_)

    def __str__(self) -> str:
        return f"Job(id={self.id_}, name={self.name}, queue={self.queue.name})"


@dataclass
class JobResult:
    success: bool
    started_when: int
    finished_when: int
    result: str  # json
    exception: Optional[str] = None
