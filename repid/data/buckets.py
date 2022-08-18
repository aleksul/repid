from dataclasses import dataclass, field
from typing import Any, Dict, Tuple, Union
from uuid import uuid4

from repid.utils import unix_time


@dataclass(frozen=True)
class ArgsBucket:
    id_: str = field(default_factory=lambda: uuid4().hex)
    args: Tuple = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)
    timestamp: int = field(default_factory=unix_time)
    ttl: Union[int, None] = None


@dataclass(frozen=True)
class ResultBucket:
    id_: str
    data: Any
    success: bool
    started_when: int
    finished_when: int
    exception: Union[str, None]
    timestamp: int
    ttl: Union[int, None]


AnyBucketT = Union[ArgsBucket, ResultBucket]
