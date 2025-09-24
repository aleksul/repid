from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from repid._utils import get_dependency
from repid._utils.asyncify import asyncify

if TYPE_CHECKING:
    from repid.dependencies.resolver_context import ResolverContext


class Depends:
    __slots__ = (
        "_fn",
        "_subdependencies",
    )

    def __init__(
        self,
        fn: Callable[..., Any | Coroutine],
        *,
        run_in_process: bool = False,
        pool_executor: Executor | None = None,
    ) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process, executor=pool_executor)
        self._update_subdependencies()

    def override(
        self,
        fn: Callable[..., Any | Coroutine],
        *,
        run_in_process: bool = False,
        pool_executor: Executor | None = None,
    ) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process, executor=pool_executor)
        self._update_subdependencies()

    async def resolve(self, *, context: ResolverContext) -> Any:
        unresolved_dependencies: dict[str, Coroutine] = {
            dep_name: dep.resolve(context=context)
            for dep_name, dep in self._subdependencies.items()
        }

        unresolved_dependencies_names, unresolved_dependencies_values = (
            unresolved_dependencies.keys(),
            unresolved_dependencies.values(),
        )

        resolved = await asyncio.gather(*unresolved_dependencies_values)

        dependency_kwargs = dict(zip(unresolved_dependencies_names, resolved, strict=False))

        return await self._fn(**dependency_kwargs)

    def _update_subdependencies(self) -> None:
        self._subdependencies = {}

        signature = inspect.signature(self._fn)

        for p in signature.parameters.values():
            if (
                p.kind == inspect.Parameter.POSITIONAL_ONLY
                and get_dependency(p.annotation) is not None
            ):
                raise ValueError("Dependencies in positional-only arguments are not supported.")

            if (
                p.kind
                in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                and (dep := get_dependency(p.annotation)) is not None
            ):
                self._subdependencies[p.name] = dep
                continue

            if p.default is inspect.Parameter.empty:
                raise ValueError("Non-dependency arguments without defaults are not supported.")
