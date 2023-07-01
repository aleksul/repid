import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from repid._utils.is_installed import is_installed

if is_installed("pydantic"):
    from pydantic import BaseModel


class _RepidJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return obj.total_seconds()
        if is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        if is_installed("pydantic") and isinstance(obj, BaseModel):
            if is_installed("pydantic", ">=2.0.0,<3.0.0"):
                return obj.model_dump(mode="json")
            return obj.dict()  # pragma: no cover
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)  # pragma: no cover


JSON_ENCODER = _RepidJSONEncoder(separators=(",", ":"))
