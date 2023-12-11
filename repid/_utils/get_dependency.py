from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, cast

if sys.version_info >= (3, 9):  # pragma: no cover
    from typing import Annotated, get_origin
else:  # pragma: no cover
    from typing_extensions import Annotated, get_origin

if TYPE_CHECKING:
    from repid.dependencies.protocols import DependencyT


def get_dependency(t: Any) -> DependencyT | None:
    # check if type is directly a depenendency
    if getattr(t, "__repid_dependency__", "") == "direct":
        return cast("DependencyT", t)
    # check if dependency is inside of Annotated
    if get_origin(t) is Annotated and (
        getattr(
            dep := t.__metadata__[0],
            "__repid_dependency__",
            "",
        )
        == "annotated"
    ):
        return cast("DependencyT", dep)
    return None
