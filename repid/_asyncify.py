from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial, wraps
from typing import Any, Callable, Coroutine, TypeVar, overload

if sys.version_info >= (3, 10):  # pragma: no cover
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

FnP = ParamSpec("FnP")
FnR = TypeVar("FnR")


@overload
def asyncify(
    fn: Callable[FnP, Coroutine[Any, Any, FnR]],
    run_in_process: bool = False,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]:
    ...


@overload
def asyncify(
    fn: Callable[FnP, FnR],
    run_in_process: bool = False,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]:
    ...


def asyncify(
    fn: Callable[FnP, FnR] | Callable[FnP, Coroutine[Any, Any, FnR]],
    run_in_process: bool = False,
) -> Callable[FnP, Coroutine[Any, Any, FnR]]:

    if asyncio.iscoroutinefunction(fn):
        return fn

    executor = ProcessPoolExecutor if run_in_process else ThreadPoolExecutor

    @wraps(fn)
    async def inner(*args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        loop = asyncio.get_running_loop()
        with executor() as pool:
            return await loop.run_in_executor(
                pool,
                partial(fn, *args, **kwargs),  # type: ignore[arg-type]
            )

    return inner
