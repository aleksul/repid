from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine, Generator
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from repid._utils import asyncify
from repid.dependencies._utils import DependencyT, get_dependency
from repid.dependencies.header_dependency import Header

if TYPE_CHECKING:
    from repid.dependencies._utils import DependencyContext


class Depends:
    __slots__ = (
        "_fn",
        "_params",
        "_simple_params",
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

    async def resolve(self, *, context: DependencyContext) -> Any:
        # Resolve sub-dependencies first
        unresolved_dependencies: dict[str, Coroutine] = {
            dep_name: dep.resolve(context=context)
            for dep_name, dep in self._subdependencies.items()
        }

        unresolved_dependencies_names = list(unresolved_dependencies.keys())
        unresolved_dependencies_values = list(unresolved_dependencies.values())

        resolved = await asyncio.gather(*unresolved_dependencies_values)

        dependency_kwargs = dict(zip(unresolved_dependencies_names, resolved, strict=False))

        # Merge simple parameters parsed by converters for this Depends instance
        simple_values = context.provided_params.get(id(self), {})
        all_kwargs = {**simple_values, **dependency_kwargs}

        return await self._fn(**all_kwargs)

    def _update_subdependencies(self) -> None:
        self._subdependencies: dict[str, DependencyT] = {}
        self._simple_params: set[str] = set()
        self._params: dict[str, inspect.Parameter] = {}

        signature = inspect.signature(self._fn)

        for p in signature.parameters.values():
            self._params[p.name] = p
            if (
                p.kind == inspect.Parameter.POSITIONAL_ONLY
                and get_dependency(p.annotation) is not None
            ):
                raise ValueError("Dependencies in positional-only arguments are not supported.")

            if p.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                dep = get_dependency(p.annotation)
                if dep is not None:
                    self._subdependencies[p.name] = dep
                else:
                    # simple param; value will be provided by converter via context
                    self._simple_params.add(p.name)

    # Expose simple parameter names for converters
    def iter_simple_params(self) -> Generator[tuple[str, inspect.Parameter], None, None]:
        for name in self._simple_params:
            yield name, self._params[name]

    # Expose header sub-dependencies for converters
    def iter_header_dependencies(
        self,
    ) -> Generator[tuple[str, Header, inspect.Parameter], None, None]:
        for name, dep in self._subdependencies.items():
            if isinstance(dep, Header):
                p = self._params[name]
                yield name, dep, p
