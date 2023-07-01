import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Union

from repid._utils import FROZEN_DATACLASS, JSON_ENCODER, SLOTS_DATACLASS


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class ArgsBucket:
    data: str

    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "ArgsBucket":
        loaded: Dict[str, Any] = json.loads(data)

        for key, value in loaded.items():
            if value is None:
                continue
            if key == "timestamp":
                loaded[key] = datetime.fromisoformat(value)
            elif key == "ttl":
                loaded[key] = timedelta(seconds=float(value))

        # drop keys of a result bucket if present
        for key in ["started_when", "finished_when", "success", "exception"]:
            loaded.pop(key, None)

        return cls(**loaded)

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now(tz=self.timestamp.tzinfo) > self.timestamp + self.ttl


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class ResultBucket:
    data: str

    # time_ns
    started_when: int
    finished_when: int

    success: bool = True
    exception: Union[str, None] = None

    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return JSON_ENCODER.encode(asdict(self))

    @classmethod
    def decode(cls, data: str) -> "ResultBucket":
        loaded: Dict[str, Any] = json.loads(data)

        for key, value in loaded.items():
            if value is None:
                continue
            if key == "timestamp":
                loaded[key] = datetime.fromisoformat(value)
            elif key == "ttl":
                loaded[key] = timedelta(seconds=float(value))

        return cls(**loaded)

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now(tz=self.timestamp.tzinfo) > self.timestamp + self.ttl
