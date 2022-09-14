from dataclasses import dataclass, field
from datetime import datetime, timedelta
from time import perf_counter_ns
from typing import Union

import orjson

from repid.data._const import SLOTS_DATACLASS


@dataclass(frozen=True, **SLOTS_DATACLASS)
class ArgsBucket:
    args: str

    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return orjson.dumps(self).decode()

    @classmethod
    def decode(cls, data: str) -> "ArgsBucket":
        return cls(**orjson.loads(data))

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now() > self.timestamp + self.ttl


@dataclass(frozen=True, **SLOTS_DATACLASS)
class ResultBucket:
    data: str

    started_when: int
    finished_when: int = field(default_factory=perf_counter_ns)

    success: bool = True
    exception: Union[str, None] = None

    timestamp: datetime = field(default_factory=datetime.now)
    ttl: Union[timedelta, None] = None

    def encode(self) -> str:
        return orjson.dumps(self).decode()

    @classmethod
    def decode(cls, data: str) -> "ResultBucket":
        return cls(**orjson.loads(data))

    @property
    def is_overdue(self) -> bool:
        if self.ttl is None:
            return False
        return datetime.now() > self.timestamp + self.ttl
