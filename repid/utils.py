import re
import time
from typing import Any, Dict, List, Union

import orjson

VALID_NAME_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9._-]*")  # shows valid actor and queue names

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

current_unix_time = lambda: int(time.time())  # noqa: E731

orjson_dumper = lambda v, *, default: orjson.dumps(v, default=default).decode()  # noqa: E731
