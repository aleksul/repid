from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, root_validator

from repid.utils import VALID_ID

try:
    from croniter import croniter

    CRON_SUPPORT = True
except ImportError:
    CRON_SUPPORT = False


class RetriesProperties(BaseModel):
    max_amount: int = 1
    already_tried: int = 0


class ResultProperties(BaseModel):
    id_: str = Field(default_factory=lambda: uuid4().hex, regex=str(VALID_ID))
    ttl: timedelta | None = None


class DelayProperties(BaseModel):
    delay_until: datetime | None = None
    defer_by: timedelta | None = None
    cron: str | None = None
    next_execution_time: datetime | None = None

    @root_validator
    def check_defer_by_with_cron(cls, values: dict[str, Any]) -> dict[str, Any]:
        defer_by = values.get("defer_by")
        cron = values.get("cron")
        if defer_by is not None and cron is not None:
            raise ValueError("Can't set defer_by and cron alongside.")
        return values


class Parameters(BaseModel):
    execution_timeout: timedelta = Field(default_factory=lambda: timedelta(minutes=10))
    result: ResultProperties | None = None
    retries: RetriesProperties | None = None
    delay: DelayProperties | None = None
    timestamp: datetime = Field(default_factory=datetime.now)
    ttl: timedelta | None = None

    def encode(self) -> str:
        return self.json(exclude_defaults=True)

    @classmethod
    def decode(cls, data: str) -> Parameters:
        return cls.parse_raw(data)

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
