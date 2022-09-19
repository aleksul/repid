from datetime import datetime, timedelta
from typing import ClassVar, Protocol, Type, TypeVar, Union, runtime_checkable
from uuid import uuid4

from repid.data.priorities import PrioritiesT

T_co = TypeVar("T_co", covariant=True)


class SerializableT(Protocol[T_co]):
    def encode(self) -> str:
        ...

    @classmethod
    def decode(cls, data: str) -> T_co:
        ...


class TimedT(Protocol):
    timestamp: datetime
    ttl: Union[timedelta, None]

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
    ) -> None:
        ...


class RetriesPropertiesT(Protocol):
    max_amount: int
    already_tried: int

    def __init__(
        self,
        *,
        max_amount: int = 1,
        already_tried: int = 0,
    ) -> None:
        ...


class ResultPropertiesT(Protocol):
    id_: str
    ttl: Union[timedelta, None]

    def __init__(
        self,
        *,
        id_: Union[str, None] = None,
        ttl: Union[timedelta, None] = None,
    ) -> None:
        ...


class DelayPropertiesT(Protocol):
    delay_until: Union[datetime, None]
    defer_by: Union[timedelta, None]
    cron: Union[str, None]
    next_execution_time: Union[datetime, None]

    def __init__(
        self,
        *,
        delay_until: Union[datetime, None] = None,
        defer_by: Union[timedelta, None] = None,
        cron: Union[str, None] = None,
        next_execution_time: Union[datetime, None] = None,
    ) -> None:
        ...


class ParametersT(SerializableT, TimedT, Protocol):
    RETRIES_CLASS: ClassVar[Type[RetriesPropertiesT]]
    RESULT_CLASS: ClassVar[Type[ResultPropertiesT]]
    DELAY_CLASS: ClassVar[Type[DelayPropertiesT]]

    execution_timeout: timedelta

    # why property? See mypy docs
    # https://mypy.readthedocs.io/en/stable/common_issues.html#covariant-subtyping-of-mutable-protocol-members-is-rejected
    @property
    def result(self) -> Union[ResultPropertiesT, None]:
        ...

    @property
    def retries(self) -> RetriesPropertiesT:
        ...

    @property
    def delay(self) -> DelayPropertiesT:
        ...

    def __init__(
        self,
        *,
        execution_timeout: timedelta = timedelta(minutes=10),
        result: Union[ResultPropertiesT, None] = None,
        retries: Union[RetriesPropertiesT, None] = None,
        delay: Union[DelayPropertiesT, None] = None,
        timestamp: datetime = datetime.now(),
        ttl: Union[timedelta, None] = None,
    ) -> None:
        ...

    @property
    def compute_next_execution_time(self) -> Union[datetime, None]:
        """Computes unix timestamp of the next execution time."""

    def _prepare_reschedule(self) -> "ParametersT":
        ...

    def _prepare_retry(self, next_retry: timedelta) -> "ParametersT":
        ...


class BucketT(SerializableT, TimedT, Protocol):
    data: str

    def __init__(
        self,
        *,
        data: str = "",
        timestamp: Union[datetime, None] = None,
        ttl: Union[timedelta, None] = None,
    ) -> None:
        ...


@runtime_checkable
class ResultBucketT(BucketT, Protocol):
    # perf_counter_ns
    started_when: int
    finished_when: int

    success: bool = True
    exception: Union[str, None] = None

    def __init__(
        self,
        *,
        data: str = "",
        started_when: int = -1,
        finished_when: int = -1,
        success: bool = True,
        exception: Union[str, None] = None,
        timestamp: Union[datetime, None] = None,
        ttl: Union[timedelta, None] = None,
    ) -> None:
        ...


class MessageT(Protocol):
    key: RoutingKeyT
    payload: str
    parameters: ParametersT

    def __init__(
        self,
        *,
        key: RoutingKeyT,
        payload: str,
        parameters: ParametersT,
    ) -> None:
        ...
