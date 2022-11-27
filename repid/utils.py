import re
import sys

VALID_ID = re.compile(r"[a-zA-Z0-9_-]+")
VALID_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_-]*")  # valid actor and queue names

SLOTS_DATACLASS = dict(slots=True) if sys.version_info >= (3, 10) else dict()
FROZEN_DATACLASS = dict(frozen=True)  # this is a hack for mypy
# for some reason directly declaring frozen dataclass makes mypy think that the dataclass isn't
# implementing the protocol
