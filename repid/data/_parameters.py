from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Union
from uuid import uuid4

import orjson

from repid.data._const import SLOTS_DATACLASS
from repid.utils import VALID_ID

try:
    from croniter import croniter

    CRON_SUPPORT = True
except ImportError:
    CRON_SUPPORT = False


@dataclass(frozen=True, **SLOTS_DATACLASS)
class RetriesProperties:
    max_amount: int = 1
    already_tried: int = 0

    def __post_init__(self) -> None:
        if self.max_amount < 1:
            raise ValueError("Incorrect max_amount.")

        if self.already_tried < 0:
            raise ValueError("Incorrect already_tried.")


@dataclass(frozen=True, **SLOTS_DATACLASS)
class ResultProperties:
    id_: str = field(default_factory=lambda: uuid4().hex)
    ttl: Union[timedelta, None] = None

    def __post_init__(self) -> None:
        if not VALID_ID.fullmatch(self.id_):
            raise ValueError("Incorrect id.")


@dataclass(frozen=True, **SLOTS_DATACLASS)
class DelayProperties:
    delay_until: Union[datetime, None] = None
    defer_by: Union[timedelta, None] = None
    cron: Union[str, None] = None
    next_execution_time: Union[datetime, None] = None

    def __post_init__(self) -> None:
        if self.defer_by is not None and self.cron is not None:
            raise ValueError("Can't set defer_by and cron alongside.")


@dataclass(frozen=True, **SLOTS_DATACLASS)
class Parameters:
    execution_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=10))
    result: Union[ResultProperties, None] = None
    retries: Union[RetriesProperties, None] = None
    delay: Union[DelayProperties, None] = None
    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return orjson.dumps(self).decode()

    @classmethod
    def decode(cls, data: str) -> "Parameters":
        loaded: Dict[str, Any] = orjson.loads(data)

        for key, value in loaded.items():
            if value is None:
                continue
            if key == "result":
                loaded[key] = ResultProperties(**orjson.loads(value))
            elif key == "retries":
                loaded[key] = RetriesProperties(**orjson.loads(value))
            elif key == "delay":
                loaded[key] = DelayProperties(**orjson.loads(value))

        return cls(**loaded)

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now() > self.timestamp + self.ttl

    @property
    def compute_next_execution_time(self) -> datetime | None:
        if self.delay is None:
            return None
        if self.delay.defer_by is not None:
            defer_by_times = (datetime.now() - self.timestamp) // self.delay.defer_by + 1
            time_offset = self.delay.defer_by * defer_by_times
            return self.timestamp + time_offset
        elif self.delay.cron is not None:
            if not CRON_SUPPORT:
                raise ImportError("Croniter is not installed.")
            return croniter(self.delay.cron, datetime.now()).get_next(ret_type=datetime)  # type: ignore[no-any-return]  # noqa: E501
        return None
