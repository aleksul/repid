from __future__ import annotations

from asyncio import create_task
from contextvars import ContextVar
from functools import wraps
from inspect import signature
from typing import Awaitable, Callable, TypeVar

from typing_extensions import ParamSpec

from . import Middleware

IsInsideMiddleware = ContextVar("IsInsideMiddleware", default=False)
FnR = TypeVar("FnR")
FnP = ParamSpec("FnP")


def MiddlewareWrapper(
    fn: Callable[FnP, Awaitable[FnR]],
    name: str | None = None,
) -> Callable[FnP, Awaitable[FnR]]:
    name = name or fn.__name__
    parameters = signature(fn).parameters.keys()

    async def call_set_context(*args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        IsInsideMiddleware.set(True)
        return await fn(*args, **kwargs)

    @wraps(fn)
    async def inner(*args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        if IsInsideMiddleware.get():
            return await fn(*args, **kwargs)
        signal_kwargs = kwargs.copy()
        signal_kwargs.update(zip(parameters, args))
        await Middleware.emit_signal(f"before_{name}", signal_kwargs)
        result = await create_task(call_set_context(*args, **kwargs))
        await Middleware.emit_signal(f"after_{name}", signal_kwargs)
        return result

    return inner
