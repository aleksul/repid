from typing import Any, Dict, Optional

import msgspec

from .utils import current_unix_time


class Message(msgspec.Struct, tag=True, omit_defaults=True):
    id_: str
    name: str  # name of the actor
    queue: str
    retries_left: int = 1
    actor_timeout: int = 600
    data: Dict[str, Any] = dict()
    timestamp: int = current_unix_time()
    ttl: Optional[int] = 86400


class DeferredMessage(msgspec.Struct, tag=True, omit_defaults=True):
    id_: str
    name: str  # name of the actor
    queue: str
    delay_until: int
    defer_by: Optional[int] = None
    retries_left: int = 1
    actor_timeout: int = 600
    data: Dict[str, Any] = dict()
    timestamp: int = current_unix_time()
    ttl: Optional[int] = None


class Result(msgspec.Struct, tag=True, omit_defaults=True):
    id_: str  # should correspond to message id
    success: bool
    started_when: int
    finished_when: int
    data: Dict[str, Any] = dict()
    exception: Optional[str] = None
    timestamp: int = current_unix_time()
    ttl: Optional[int] = 86400
