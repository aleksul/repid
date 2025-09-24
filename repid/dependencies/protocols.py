from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from repid.dependencies.resolver_context import ResolverContext


@runtime_checkable
class DependencyT(Protocol):
    async def resolve(self, *, context: ResolverContext) -> Any: ...
