import json
from datetime import date, datetime, time, timedelta
from typing import Any


class _RepidJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return str(obj.total_seconds())
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)  # pragma: no cover


JSON_ENCODER = _RepidJSONEncoder(separators=(",", ":"))
