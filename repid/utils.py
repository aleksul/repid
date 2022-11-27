import re
import time

VALID_ID = re.compile(r"[a-zA-Z0-9_-]*")
VALID_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_-]*")  # valid actor and queue names


def unix_time() -> int:
    return int(time.time())
