import re
import time

from repid.data import PrioritiesT

VALID_ID = re.compile(r"[a-zA-Z0-9_-]*")
VALID_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_-]*")  # valid actor and queue names
VALID_PRIORITIES = re.compile(r"[0-9]+\/[0-9]+\/[0-9]+")


def unix_time() -> int:
    return int(time.time())


def queue_name_constructor(
    name: str,
    priority: PrioritiesT = PrioritiesT.MEDIUM,
    delayed: bool = False,
    dead: bool = False,
) -> str:
    return f"q:{name}:{priority.value}:{'d' if delayed else 'n'}{':dead' if dead else ''}"


def message_name_constructor(
    queue: str,
    id_: str,
) -> str:
    return f"m:{queue}:{id_}"
