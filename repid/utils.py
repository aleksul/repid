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

def unix_time() -> int:
    return int(time.time())


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
