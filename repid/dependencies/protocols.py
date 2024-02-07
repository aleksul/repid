from __future__ import annotations

import sys
from enum import Enum
from typing import Any, Protocol, Union

if sys.version_info >= (3, 11):  # pragma: no cover
    from typing import Self
else:
    from typing_extensions import Self

from repid.dependencies.resolver_context import ResolverContext


class DependencyKind(str, Enum):
    DIRECT = "direct"
    ANNOTATED = "annotated"


class DirectDependencyT(Protocol):
    __repid_dependency__: DependencyKind = DependencyKind.DIRECT

    @classmethod
    def construct_as_dependency(cls, *, context: ResolverContext) -> Self: ...

    async def resolve(self) -> Any: ...


class AnnotatedDependencyT(Protocol):
    __repid_dependency__: DependencyKind = DependencyKind.ANNOTATED

    async def resolve(self, *, context: ResolverContext) -> Any: ...


DependencyT = Union[DirectDependencyT, AnnotatedDependencyT]
