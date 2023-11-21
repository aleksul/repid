from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Coroutine

from repid._asyncify import asyncify
from repid.dependencies.protocols import DependencyKind

if TYPE_CHECKING:
    from repid.dependencies.resolver_context import ResolverContext


class Depends:
    __repid_dependency__ = DependencyKind.ANNOTATED

    __slots__ = ("_fn",)

    def __init__(self, fn: Callable[[], Any | Coroutine], *, run_in_process: bool = False) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process)

    def override(self, fn: Callable[[], Any | Coroutine], *, run_in_process: bool = False) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process)

    async def resolve(self, *, context: ResolverContext) -> Any:  # noqa: ARG002
        return await self._fn()
