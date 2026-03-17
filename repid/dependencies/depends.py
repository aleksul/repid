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
        "_fn_locals",
        "_params",
        "_payload_arguments",
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

        self._fn_locals = None
        current_frame = inspect.currentframe()
        if current_frame is not None:
            previous_frame = current_frame.f_back
            if previous_frame is not None:
                self._fn_locals = previous_frame.f_locals

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
        unresolved_dependencies: dict[str, Coroutine] = {
            dep_name: dep.resolve(context=context)
            for dep_name, dep in self._subdependencies.items()
        }

        unresolved_dependencies_names = list(unresolved_dependencies.keys())
        unresolved_dependencies_values = list(unresolved_dependencies.values())

        resolved = await asyncio.gather(*unresolved_dependencies_values)

        dependency_kwargs = dict(zip(unresolved_dependencies_names, resolved, strict=False))

        payload_arguments = {}
        for name in self._payload_arguments:
            param = self._params[name]
            if name in context.parsed_kwargs:
                payload_arguments[name] = context.parsed_kwargs[name]
            elif param.default is not inspect.Parameter.empty:
                payload_arguments[name] = param.default
            else:
                raise ValueError(f"Missing required argument '{name}' in payload.")

        return await self._fn(**{**payload_arguments, **dependency_kwargs})

    def _update_subdependencies(self) -> None:
        self._subdependencies: dict[str, DependencyT] = {}
        self._payload_arguments: set[str] = set()
        self._params: dict[str, inspect.Parameter] = {}

        signature = inspect.signature(
            self._fn,
            eval_str=True,
            locals=self._fn_locals,
            globals=self._fn.__globals__,
        )

        for p in signature.parameters.values():
            self._params[p.name] = p
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                raise ValueError("Positional-only arguments in dependencies are not supported.")
            if p.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                dep = get_dependency(p.annotation)
                if dep is not None:
                    self._subdependencies[p.name] = dep
                else:
                    self._payload_arguments.add(p.name)

    def _iter_payload_arguments(self) -> Generator[inspect.Parameter, None, None]:
        for name in self._payload_arguments:
            yield self._params[name]
        for _sub_name, dep in self._subdependencies.items():
            if isinstance(dep, Depends):
                yield from dep._iter_payload_arguments()

    def _iter_header_arguments(self) -> Generator[tuple[Header, inspect.Parameter], None, None]:
        for name, dep in self._subdependencies.items():
            if isinstance(dep, Header):
                p = self._params[name]
                yield dep, p
            elif isinstance(dep, Depends):
                yield from dep._iter_header_arguments()
