from dataclasses import dataclass
from typing import Any, Dict, Tuple, Union


@dataclass()
class ArgsBucket:
    id_: str
    args: Union[Tuple, None]
    kwargs: Union[Dict, None]
    timestamp: int
    ttl: Union[int, None]


@dataclass()
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
