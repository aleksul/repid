from __future__ import annotations

from typing import Annotated, Any, get_origin

from repid.dependencies.protocols import DependencyT


def get_dependency(t: Any) -> DependencyT | None:
    # check if type is directly a depenendency
    if isinstance(t, DependencyT):
        return t
    # check if dependency is inside of Annotated
    if get_origin(t) is Annotated and isinstance(dep := t.__metadata__[0], DependencyT):
        return dep
    return None
