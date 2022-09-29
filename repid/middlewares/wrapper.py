from __future__ import annotations

import sys
from asyncio import create_task
from contextvars import ContextVar
from functools import update_wrapper
from inspect import signature
from typing import Any, Awaitable, Callable, Generic, TypeVar

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

IsInsideMiddleware = ContextVar("IsInsideMiddleware", default=False)
FnR = TypeVar("FnR")
FnP = ParamSpec("FnP")


class MiddlewareWrapper(Generic[FnP, FnR]):
    def __init__(self, fn: Callable[FnP, Awaitable[FnR]]) -> None:
        update_wrapper(self, fn)
        self.fn = fn
        self.name = fn.__name__
        self.parameters = signature(fn).parameters.keys()
        self._repid_signal_emitter: Callable[[str, dict[str, Any]], Awaitable] | None = None

    async def call_set_context(self, *args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        IsInsideMiddleware.set(True)
        return await self.fn(*args, **kwargs)

    async def __call__(self, *args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        if IsInsideMiddleware.get() or self._repid_signal_emitter is None:
            return await self.fn(*args, **kwargs)
        signal_kwargs = kwargs.copy()
        signal_kwargs.update(zip(self.parameters, args))
        await self._repid_signal_emitter(f"before_{self.name}", signal_kwargs)
        result = await create_task(self.call_set_context(*args, **kwargs))
        signal_kwargs.update(dict(returns=result))
        await self._repid_signal_emitter(f"after_{self.name}", signal_kwargs)
        return result
