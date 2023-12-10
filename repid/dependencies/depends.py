from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Callable, Coroutine, cast

from repid._asyncify import asyncify
from repid._utils import get_dependency
from repid.dependencies.protocols import DependencyKind

if TYPE_CHECKING:
    from repid.dependencies.protocols import AnnotatedDependencyT, DirectDependencyT
    from repid.dependencies.resolver_context import ResolverContext


class Depends:
    __repid_dependency__ = DependencyKind.ANNOTATED

    __slots__ = (
        "_fn",
        "_subdependencies",
    )

    def __init__(self, fn: Callable[..., Any | Coroutine], *, run_in_process: bool = False) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process)
        self._update_subdependencies()

    def override(self, fn: Callable[..., Any | Coroutine], *, run_in_process: bool = False) -> None:
        self._fn = asyncify(fn, run_in_process=run_in_process)
        self._update_subdependencies()

    async def resolve(self, *, context: ResolverContext) -> Any:
        unresolved_dependencies: dict[str, Coroutine] = {}

        for dep_name, dep in self._subdependencies.items():
            dep_kind = getattr(dep, "__repid_dependency__", "")
            if dep_kind == DependencyKind.DIRECT:
                unresolved_dependencies[dep_name] = (
                    cast("DirectDependencyT", dep)
                    .construct_as_dependency(context=context)
                    .resolve()
                )
            elif dep_kind == DependencyKind.ANNOTATED:
                unresolved_dependencies[dep_name] = cast("AnnotatedDependencyT", dep).resolve(
                    context=context,
                )
            else:  # pragma: no cover
                # this should never happen
                raise ValueError("Unsupported dependency argument.")

        unresolved_dependencies_names, unresolved_dependencies_values = (
            unresolved_dependencies.keys(),
            unresolved_dependencies.values(),
        )

        resolved = await asyncio.gather(*unresolved_dependencies_values)

        dependency_kwargs = dict(zip(unresolved_dependencies_names, resolved))

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
