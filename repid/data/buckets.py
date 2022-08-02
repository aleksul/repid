from typing import Any, Dict, Optional, Tuple

from repid.data import StructWithParams, Timestamp


class Bucket(StructWithParams, Timestamp):
    id_: str


class ArgsBucket(Bucket):
    args: Optional[Tuple] = None
    kwargs: Optional[Dict] = None


class ResultBucket(Bucket):
    data: Any
    success: bool
    started_when: int
    finished_when: int
    exception: Optional[str] = None
