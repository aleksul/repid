import dataclasses
import enum
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

# This re shows valid actor and queue names.
VALID_NAME_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9._-]*")

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class JobStatus(enum.Enum):
    QUEUED = 1
    PROCESSING = 2
    DONE = 3
    DEAD = 4
    NOT_FOUND = 5


@dataclasses.dataclass
class JobResult:
    id_: str  # corresponding to the job's id
    success: bool
    started_when: int
    finished_when: int
    result: JSONType
    # Timestamp
    created: int = dataclasses.field(default_factory=lambda: int(datetime.now().timestamp()))
    updated: int = dataclasses.field(default_factory=lambda: int(datetime.now().timestamp()))
    ttl: Optional[int] = None  # time-to-live in seconds


@dataclasses.dataclass
class JobData:
    name: str
    # Queue parameters
    queue: str = "default"
    priority: Literal["HIGH", "DEFERRED", "NORMAL", "LOW"] = "NORMAL"
    dead: bool = False
    # Retry parameters
    retries: int = 1
    retries_left: int = 1
    timeout: int = 600  # job timeout in seconds, 10 minutes by default
    # Timestamp
    created: int = dataclasses.field(default_factory=lambda: int(datetime.now().timestamp()))
    updated: int = dataclasses.field(default_factory=lambda: int(datetime.now().timestamp()))
    deferred_until: Optional[int] = None  # time of job execution in seconds since the epoch
    deferred_by: Optional[int] = None  # schedule job every N seconds
    next_exec_time: Optional[int] = None
    ttl: Optional[int] = None  # time-to-live in seconds
    # Parameters, that will be passed to the corresponding function
    func_name: Optional[str] = None
    args: Optional[JSONType] = None
    kwargs: Optional[Dict[str, JSONType]] = None
    # ID - NOTE: job with the same id can NOT be scheduled multiple times
    id_: Optional[str] = None

    def __post_init__(self):
        if self.id_ is None:
            self.id_ = f"{self.name}:{uuid.uuid4().hex}"
        if self.func_name is None:
            self.func_name = self.name
        if self.deferred_by is not None and self.next_exec_time is None:
            self.next_exec_time = int(datetime.now().timestamp()) + self.deferred_by

        if not all(VALID_NAME_RE.fullmatch(v) for v in [self.name, self.queue]):
            raise ValueError(
                "Job and Queue names must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )

        is_deferred: bool = any(v is not None for v in [self.deferred_by, self.deferred_until])
        if is_deferred and self.priority != "DEFERRED":
            raise ValueError("Job with deferred_by or deferred_until must have priority='DEFERRED.")

        if self.deferred_by is not None and self.deferred_until is not None:
            raise ValueError("Job can't be both deferred_by and deferred_until.")

        if self.retries < self.retries_left:
            raise ValueError("Number of 'retries' must be greater or equal to 'retries_left'.")

        if self.retries_left == 0 and not self.dead:
            raise ValueError("Job with no 'retries_left' must be dead.")

        if 0 < self.retries_left < self.retries and self.next_exec_time is None:
            raise ValueError("Retried job must have 'next_exec_time'.")


@dataclasses.dataclass
class QueueData:
    name: str
    high_priority: Tuple[str]
    normal_priority: Tuple[str]
    deferred: Dict[int, str]
    low_priority: Tuple[str]
    dead: Tuple[str]
