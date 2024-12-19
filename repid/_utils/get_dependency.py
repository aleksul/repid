from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, cast, get_origin

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
