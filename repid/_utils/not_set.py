from typing import Any


class _NotSet:
    def __repr__(self) -> str:  # pragma: no cover
        return "NotSet"


NotSet: Any = _NotSet()
