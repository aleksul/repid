import random
import re
import time
from typing import List, Union

from repid.data import PrioritiesT

try:
    from croniter import croniter
except ImportError:

    class croniter:  # type: ignore[no-redef]
        raise ImportError("Please install croniter.")


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
        return int(croniter(msg.cron, time.time()).get_next(ret_type=float))


def get_priorities_order(priorities_distribution: List[float]) -> List[PrioritiesT]:
    rand = random.random()
    if rand <= priorities_distribution[0]:
        return [PrioritiesT.HIGH, PrioritiesT.MEDIUM, PrioritiesT.LOW]
    elif rand <= priorities_distribution[0] + priorities_distribution[1]:
        return [PrioritiesT.MEDIUM, PrioritiesT.HIGH, PrioritiesT.LOW]
    else:
        return [PrioritiesT.LOW, PrioritiesT.HIGH, PrioritiesT.MEDIUM]


def parse_priorities_distribution(priorities_distribution: str) -> List[float]:
    if not VALID_PRIORITIES.fullmatch(priorities_distribution):
        raise ValueError(f"Invalid priorities distribution: {priorities_distribution}")
    pr_dist = [int(x) for x in priorities_distribution.split("/")]
    pr_dist_sum = sum(pr_dist)
    return [x / pr_dist_sum for x in pr_dist]
