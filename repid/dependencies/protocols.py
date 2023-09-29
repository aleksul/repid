from __future__ import annotations

import sys
from typing import Any, Protocol, runtime_checkable

if sys.version_info >= (3, 9):  # pragma: no cover
    from typing import Annotated, get_origin
else:  # pragma: no cover
    from typing_extensions import Annotated, get_origin


@runtime_checkable
class DependencyT(Protocol):
    __repid_dependency__: bool = True


def get_dependency(t: Any) -> DependencyT | None:
    if isinstance(t, DependencyT):
        return t
    if get_origin(t) is Annotated and isinstance(dep := t.__metadata__[0], DependencyT):
        return dep
    return None
