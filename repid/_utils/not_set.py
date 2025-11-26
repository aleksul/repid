from typing import Any


class _NotSet:
    def __repr__(self) -> str:
        return "NotSet"


NotSet: Any = _NotSet()
