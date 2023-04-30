import json
import re
import sys
from datetime import datetime, timedelta
from typing import Any

VALID_ID = re.compile(r"[a-zA-Z0-9_-]+")
VALID_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_-]*")  # valid actor and queue names

SLOTS_DATACLASS = {"slots": True} if sys.version_info >= (3, 10) else {}
FROZEN_DATACLASS = {"frozen": True}  # this is a hack for mypy
# for some reason directly declaring frozen dataclass makes mypy think that the dataclass isn't
# implementing the protocol


class _RepidJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return str(obj.total_seconds())
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)  # pragma: no cover


JSON_ENCODER = _RepidJSONEncoder(separators=(",", ":"))
