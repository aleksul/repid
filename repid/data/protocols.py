from datetime import datetime, timedelta
from typing import ClassVar, Protocol, TypeVar, runtime_checkable
from uuid import uuid4

from repid.data.priorities import PrioritiesT

T_co = TypeVar("T_co", covariant=True)


class SerializableT(Protocol[T_co]):
    def encode(self) -> str: ...

    @classmethod
    def decode(cls, data: str) -> T_co: ...


class TimedT(Protocol):
    timestamp: datetime
    ttl: timedelta | None

    @property
    def is_overdue(self) -> bool:
        """Is time-to-live expired?"""


class RoutingKeyT(Protocol):
    id_: str
    topic: str
    queue: str
    priority: int

    def __init__(
        self,
        *,
        id_: str = uuid4().hex,
        topic: str,
        queue: str = "default",
        priority: int = PrioritiesT.MEDIUM.value,
    ) -> None: ...


class RetriesPropertiesT(Protocol):
    max_amount: int
    already_tried: int

    def __init__(
        self,
        *,
        max_amount: int = 0,
        already_tried: int = 0,
    ) -> None: ...


class ResultPropertiesT(Protocol):
    id_: str
    ttl: timedelta | None

    def __init__(
        self,
        *,
        id_: str | None = None,
        ttl: timedelta | None = None,
    ) -> None: ...


class DelayPropertiesT(Protocol):
    delay_until: datetime | None
    defer_by: timedelta | None
    cron: str | None
    next_execution_time: datetime | None

    def __init__(
        self,
        *,
        delay_until: datetime | None = None,
        defer_by: timedelta | None = None,
        cron: str | None = None,
        next_execution_time: datetime | None = None,
    ) -> None: ...


class ParametersT(SerializableT, TimedT, Protocol):
    RETRIES_CLASS: ClassVar[type[RetriesPropertiesT]]
    RESULT_CLASS: ClassVar[type[ResultPropertiesT]]
    DELAY_CLASS: ClassVar[type[DelayPropertiesT]]

    execution_timeout: timedelta

    # why property? See mypy docs
    # https://mypy.readthedocs.io/en/stable/common_issues.html#covariant-subtyping-of-mutable-protocol-members-is-rejected
    @property
    def result(self) -> ResultPropertiesT | None: ...

    @property
    def retries(self) -> RetriesPropertiesT: ...

    @property
    def delay(self) -> DelayPropertiesT: ...

    def __init__(
        self,
        *,
        execution_timeout: timedelta = timedelta(minutes=10),
        result: ResultPropertiesT | None = None,
        retries: RetriesPropertiesT | None = None,
        delay: DelayPropertiesT | None = None,
        timestamp: datetime = datetime.now(),  # noqa: B008
        ttl: timedelta | None = None,
    ) -> None: ...

    @property
    def compute_next_execution_time(self) -> datetime | None:
        """Computes unix timestamp of the next execution time."""

    def _prepare_reschedule(self) -> "ParametersT": ...

    def _prepare_retry(self, next_retry: timedelta) -> "ParametersT": ...


class BucketT(SerializableT, TimedT, Protocol):
    data: str

    def __init__(
        self,
        *,
        data: str = "",
        timestamp: datetime | None = None,
        ttl: timedelta | None = None,
    ) -> None: ...


@runtime_checkable
class ResultBucketT(BucketT, Protocol):
    # perf_counter_ns
    started_when: int
    finished_when: int

    success: bool = True
    exception: str | None = None

    def __init__(
        self,
        *,
        data: str = "",
        started_when: int = -1,
        finished_when: int = -1,
        success: bool = True,
        exception: str | None = None,
        timestamp: datetime | None = None,
        ttl: timedelta | None = None,
    ) -> None: ...
