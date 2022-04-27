import re
import time
from typing import TYPE_CHECKING, Union

from repid.data import PrioritiesT

if TYPE_CHECKING:
    from repid.data import DeferredByMessage, DeferredCronMessage

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


def next_exec_time(msg: Union[DeferredByMessage, DeferredCronMessage]) -> int:
    """Counts next execution time of the message.
    Depends on current time.

    Args:
        msg (DeferredByMessage | DeferredCronMessage): message to count next execution time for.

    Returns:
        int: next execution time in seconds since epoch.
    """
    if isinstance(msg, DeferredByMessage):
        defer_by_times = (unix_time() - msg.timestamp) // msg.defer_by + 1
        time_offset = msg.defer_by * defer_by_times
        return msg.timestamp + time_offset
    elif isinstance(msg, DeferredCronMessage):
        msg.cron.split(" ")
        # TODO: count next cron
        return 0
