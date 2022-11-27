from __future__ import annotations

import sys
from asyncio import create_task
from contextvars import ContextVar
from functools import partial, update_wrapper
from inspect import signature
from typing import Any, Callable, Coroutine, Generic, TypeVar, overload

if sys.version_info >= (3, 10):  # pragma: no cover
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

IsInsideMiddleware = ContextVar("IsInsideMiddleware", default=False)
FnR = TypeVar("FnR")
FnP = ParamSpec("FnP")


@overload
def middleware_wrapper(
    *,
    name: str | None = None,
) -> partial[_middleware_wrapper]:
    ...


@overload
def middleware_wrapper(
    fn: Callable[FnP, Coroutine[Any, Any, FnR]],
    name: str | None = None,
) -> _middleware_wrapper[FnP, FnR]:
    ...


def middleware_wrapper(
    fn: Callable[FnP, Coroutine[Any, Any, FnR]] | None = None,
    name: str | None = None,
) -> _middleware_wrapper[FnP, FnR] | partial[_middleware_wrapper]:
    """Decorator for adding middleware signals emitting to a function.
    Can be used as a normal function and as bracket-less/-full decorator.

    Args:
        fn (Callable[FnP, Coroutine[Any, Any, FnR]] | None, optional):
            Any async function. Optional if you use the function as a decorator. Positional-only.
        name (str | None, optional):
            Name, that will be used to call middleware signals. Defaults to name of the function.

    Returns:
        Callable[FnP, Coroutine[Any, Any, FnR]]:
            Function with the same signature as the initial one with middleware signal calls
            added before and after the call of the initial function. For the middleware to be
            actually called you have to supply `_repid_signal_emitter` after initialization.
    """
    if fn is None:
        return partial(middleware_wrapper, name=name)
    return _middleware_wrapper(fn=fn, name=name)


class _middleware_wrapper(Generic[FnP, FnR]):
    def __init__(
        self,
        fn: Callable[FnP, Coroutine[Any, Any, FnR]],
        name: str | None = None,
    ) -> None:
        update_wrapper(self, fn)
        self.fn = fn
        self.name = name or fn.__name__
        self.parameters = signature(fn).parameters.keys()
        self._repid_signal_emitter: Callable[[str, dict[str, Any]], Coroutine] | None = None

    async def call_set_context(self, *args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        IsInsideMiddleware.set(True)
        return await self.fn(*args, **kwargs)

    async def __call__(self, *args: FnP.args, **kwargs: FnP.kwargs) -> FnR:
        # check if we are already inside of a middleware call or
        # if the emitter isn't even set; then just call function normally
        if IsInsideMiddleware.get() or self._repid_signal_emitter is None:
            return await self.fn(*args, **kwargs)

        # map arguments to their names
        signal_kwargs = kwargs.copy()
        signal_kwargs.update(zip(self.parameters, args))

        # emit `before` signal
        await self._repid_signal_emitter(f"before_{self.name}", signal_kwargs)

        # run function inside of a separate context created by `asyncio.create_task()`
        # inside of this context IsInsideMiddleware variable will be set to True
        result = await create_task(self.call_set_context(*args, **kwargs))
        # whatever the function returns can be seen as `result` kwarg in `after` signal
        signal_kwargs.update(dict(result=result))

        # emit `after` signal
        await self._repid_signal_emitter(f"after_{self.name}", signal_kwargs)

        return result
