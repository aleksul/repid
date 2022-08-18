import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Tuple, Union

from repid.utils import unix_time

try:
    from croniter import croniter

    CRON_SUPPORT = True
except ImportError:
    CRON_SUPPORT = False


class PrioritiesT(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


@dataclass(frozen=True)
class Message:
    id_: str
    topic: str  # the same as actor's & job's name
    queue: str
    priority: int

    execution_timeout: int

    retries: int
    tried: int

    # args_bucket
    args_bucket_id: Union[str, None]
    simple_args: Union[Tuple, None]
    simple_kwargs: Union[Dict, None]
    # result_bucket
    result_bucket_id: Union[str, None]
    result_bucket_ttl: Union[int, None]

    delay_until: Union[int, None]
    defer_by: Union[int, None]
    cron: Union[str, None]

    timestamp: int
    ttl: Union[int, None]

    def __post_init__(self) -> None:
        if self.is_deferred and self.delay_until is None:
            object.__setattr__(self, "delay_until", self.next_execution_time)

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
            if not CRON_SUPPORT:
                raise ImportError("Croniter is not installed.")
            return int(croniter(self.cron, time.time()).get_next(ret_type=float))
        return -1

    def _prepare_reschedule(self) -> None:
        object.__setattr__(self, "tried", 0)
        object.__setattr__(self, "delay_until", self.next_execution_time)
        object.__setattr__(self, "timestamp", unix_time())

    def _increment_retry(self) -> None:
        object.__setattr__(self, "tried", self.tried + 1)
