import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Tuple, Union

from repid.utils import unix_time

try:
    from croniter import croniter
except ImportError:

    class croniter:  # type: ignore[no-redef]
        raise ImportError("Please install croniter.")


class PrioritiesT(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


@dataclass()
class ArgsBucketMetadata:
    id_: str


@dataclass()
class SimpleArgsBucket:
    args: Union[Tuple, None]
    kwargs: Union[Dict, None]


@dataclass()
class ResultBucketMetadata:
    id_: str
    ttl: Union[int, None]


@dataclass()
class Message:
    id_: str
    topic: str  # the same as actor's & job's name
    queue: str
    priority: PrioritiesT

    execution_timeout: int

    retries: int
    tried: int

    args_bucket: Union[ArgsBucketMetadata, SimpleArgsBucket, None]
    result_bucket: Union[ResultBucketMetadata, None]

    delay_until: Union[int, None]
    defer_by: Union[int, None]
    cron: Union[str, None]

    timestamp: int
    ttl: Union[int, None]

    @property
    def is_deferred(self) -> bool:
        return any((self.delay_until, self.defer_by, self.cron))

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return unix_time() > self.timestamp + self.ttl
        return False

    @property
    def next_execution_time(self) -> int:
        if self.defer_by is not None:
            defer_by_times = (unix_time() - self.timestamp) // self.defer_by + 1
            time_offset = self.defer_by * defer_by_times
            return self.timestamp + time_offset
        elif self.cron is not None:
            return int(croniter(self.cron, time.time()).get_next(ret_type=float))
        return -1

    def _prepare_reschedule(self) -> None:
        self.tried = 0
        self.delay_until = self.next_execution_time
        self.timestamp = unix_time()
