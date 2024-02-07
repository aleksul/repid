import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Type, Union
from uuid import uuid4

from repid._utils import FROZEN_DATACLASS, JSON_ENCODER, SLOTS_DATACLASS, is_installed

if TYPE_CHECKING:
    from repid.data.protocols import (
        DelayPropertiesT,
        ResultPropertiesT,
        RetriesPropertiesT,
    )

if CRON_SUPPORT := is_installed("croniter"):
    from croniter import croniter


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class RetriesProperties:
    max_amount: int = 0
    already_tried: int = 0

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "RetriesProperties":
        loaded: Dict[str, Any] = json.loads(data) if not isinstance(data, Dict) else data
        return cls(**loaded)


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class ResultProperties:
    id_: str = field(default_factory=lambda: uuid4().hex)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "ResultProperties":
        loaded: Dict[str, Any] = json.loads(data) if not isinstance(data, Dict) else data

        if (ttl := loaded.get("ttl")) is not None:
            loaded["ttl"] = timedelta(seconds=float(ttl))

        return cls(**loaded)


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class DelayProperties:
    delay_until: Union[datetime, None] = None
    defer_by: Union[timedelta, None] = None
    cron: Union[str, None] = None
    next_execution_time: Union[datetime, None] = None

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "DelayProperties":
        loaded: Dict[str, Any] = json.loads(data) if not isinstance(data, Dict) else data

        for key, value in loaded.items():
            if value is None:
                continue
            if key == "delay_until":
                loaded[key] = datetime.fromisoformat(value)
            elif key == "defer_by":
                loaded[key] = timedelta(seconds=float(value))
            elif key == "next_execution_time":
                loaded[key] = datetime.fromisoformat(value)

        return cls(**loaded)


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class Parameters:
    RETRIES_CLASS: ClassVar[Type["RetriesPropertiesT"]] = RetriesProperties
    RESULT_CLASS: ClassVar[Type["ResultPropertiesT"]] = ResultProperties
    DELAY_CLASS: ClassVar[Type["DelayPropertiesT"]] = DelayProperties

    execution_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=10))
    result: Union[ResultProperties, None] = None
    retries: RetriesProperties = field(default_factory=RetriesProperties)
    delay: DelayProperties = field(default_factory=DelayProperties)
    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "Parameters":
        loaded: Dict[str, Any] = json.loads(data)

        for key, value in loaded.items():
            if value is None:
                continue
            if key == "result":
                loaded[key] = ResultProperties.decode(value)
            elif key == "retries":
                loaded[key] = RetriesProperties.decode(value)
            elif key == "delay":
                loaded[key] = DelayProperties.decode(value)
            elif key in ["execution_timeout", "ttl"]:
                loaded[key] = timedelta(seconds=float(value))
            elif key == "timestamp":
                loaded[key] = datetime.fromisoformat(value)

        return cls(**loaded)

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now(tz=self.timestamp.tzinfo) > self.timestamp + self.ttl

    @property
    def compute_next_execution_time(self) -> Union[datetime, None]:
        now = datetime.now()
        if self.delay.delay_until is not None and self.delay.delay_until > now:
            return self.delay.delay_until
        if self.delay.defer_by is not None:
            defer_by_times = (now - self.timestamp) // self.delay.defer_by + 1
            time_offset = self.delay.defer_by * defer_by_times
            return self.timestamp + time_offset
        if self.delay.cron is not None:
            if not CRON_SUPPORT:
                raise ImportError("Croniter is not installed.")  # pragma: no cover
            return croniter(self.delay.cron, now).get_next(ret_type=datetime)  # type: ignore[no-any-return]
        return None

    def _prepare_reschedule(self) -> "Parameters":
        copy = deepcopy(self)
        object.__setattr__(copy.retries, "already_tried", 0)
        object.__setattr__(copy.delay, "next_execution_time", self.compute_next_execution_time)
        object.__setattr__(copy, "timestamp", datetime.now())
        return copy

    def _prepare_retry(self, next_retry: timedelta) -> "Parameters":
        copy = deepcopy(self)
        object.__setattr__(copy.retries, "already_tried", copy.retries.already_tried + 1)
        object.__setattr__(
            copy.delay,
            "next_execution_time",
            datetime.now() + next_retry,
        )
        return copy
