from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Coroutine
from concurrent.futures import Executor, ThreadPoolExecutor
from functools import cache, partial, wraps
from typing import Any, ParamSpec, TypeVar, overload

if sys.platform != "emscripten":  # pragma: no cover
    from concurrent.futures import ProcessPoolExecutor
else:  # pragma: no cover
    ProcessPoolExecutor = ThreadPoolExecutor

FnP = ParamSpec("FnP")
FnR = TypeVar("FnR")


@cache
def process_pool_executor() -> Executor:
    return ProcessPoolExecutor()


@cache
def thread_pool_executor() -> Executor:
    return ThreadPoolExecutor()


@overload
def asyncify(
    fn: Callable[FnP, Coroutine[Any, Any, FnR]],
    *,
    run_in_process: bool = False,
    executor: Executor | None = None,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]: ...


@overload
def asyncify(
    fn: Callable[FnP, FnR],
    *,
    run_in_process: bool = False,
    executor: Executor | None = None,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]: ...


def asyncify(
    fn: Callable[FnP, FnR] | Callable[FnP, Coroutine[Any, Any, FnR]],
    *,
    run_in_process: bool = False,
    executor: Executor | None = None,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]:
    if asyncio.iscoroutinefunction(fn):
        return fn

    executor = (
        executor
        if executor is not None
        else (process_pool_executor() if run_in_process else thread_pool_executor())
    )

    @wraps(fn)
    async def inner(*args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            executor,
            partial(fn, *args, **kwargs),  # type: ignore[arg-type]
        )

    return inner
