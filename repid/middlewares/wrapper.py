import inspect
import traceback
from functools import wraps
from typing import Any, Callable, Dict, Tuple

from repid.middlewares.middleware import Middleware, available_functions


class MiddlewareWrapperContextManager:
    def __init__(self, fn: Callable, *args: Tuple, **kwargs: Dict) -> None:
        self.fn, self.args, self.kwargs = fn, args, kwargs
        sig = inspect.signature(fn)
        self.signal_kwargs = kwargs.copy()
        mapped_args: Dict[str, Any] = {k: v for k, v in zip(sig.parameters.keys(), args)}
        if mapped_args:
            self.signal_kwargs.update(mapped_args)

    async def __aenter__(self) -> None:
        await Middleware.emit_signal(f"before_{self.fn.__name__}", self.signal_kwargs)

    async def __aexit__(self, *exc: Tuple) -> None:
        if not exc:
            await Middleware.emit_signal(f"after_{self.fn.__name__}", self.signal_kwargs)


class MiddlewareWrapper:
    __slots__ = ("fn", "args", "kwargs")

    def __init__(self, fn: Callable):
        self.fn = fn

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> Any:
        stack_frame = traceback.extract_stack(limit=3)[0]
        if (
            stack_frame.filename.endswith("repid/middlewares/middleware.py")
            and stack_frame.lineno == 94
        ):
            # this means that we already have called a middleware
            # so we don't need to call it again
            return await self.fn(*args, **kwargs)

        async with MiddlewareWrapperContextManager(self.fn, *args, **kwargs):
            return await self.fn(*args, **kwargs)


def AddMiddleware(cls):
    @wraps(cls)
    def wrapper(*args, **kwargs):
        inst = cls(*args, **kwargs)
        for key, attr in inspect.getmembers(inst, predicate=inspect.ismethod):
            if callable(attr) and attr.__name__ in available_functions:
                wrapped = MiddlewareWrapper(attr)
                setattr(inst, key, wrapped)
        return inst

    return wrapper
