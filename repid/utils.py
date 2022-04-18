import re
import time
from enum import Enum
from typing import Optional
from uuid import uuid4

VALID_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9._-]*")  # shows valid actor and queue names
VALID_PRIORITIES = re.compile(r"[0-9]+\/[0-9]+\/[0-9]+")


current_unix_time = lambda: int(time.time())  # noqa: E731


class PrioritiesT(Enum):
    HIGH = "hi"
    MEDIUM = "me"
    LOW = "lo"


def queue_name_constructor(
    name: str,
    priority: PrioritiesT = PrioritiesT.MEDIUM,
    delayed: bool = False,
) -> str:
    return f"q:{name}:{priority.value}:{'d' if delayed else 'n'}"


def message_name_constructor(
    name: str,
    queue: str,
    id_: Optional[str] = None,
) -> str:
    return f"m:{queue}:{name}:{uuid4().hex if id_ is None else id_}"
