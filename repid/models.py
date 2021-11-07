from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4


class JobStatus(Enum):
    QUEUED = 1
    IN_PROGRESS = 2
    DONE = 3
    NOT_FOUND = 4


@dataclass
class JobDefenition:
    name: str
    queue: str = "default"
    kwargs: Dict[str, Any] = field(default_factory=dict)  # will be passed to the worker function
    retries: int = 1
    defer_until: Optional[datetime] = None
    defer_by: Optional[timedelta] = None
    _id: str = None  # type: ignore

    def __post_init__(self):
        if self._id is None:
            self._id = f"{self.name}:{uuid4()}"
        if bool(self.defer_until and self.defer_by):
            raise ValueError("Usage of 'defer_until' AND 'defer_by' together is prohibited.")


@dataclass
class JobResult:
    success: Optional[bool]
    started_when: datetime
    finished_when: Optional[datetime]
    result: Any
