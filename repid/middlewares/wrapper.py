import inspect
import traceback
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Tuple, Type

from repid.middlewares.middleware import Middleware, available_functions


class MiddlewareWrapperContextManager:
    def __init__(self, fn: Callable, *args: Tuple, **kwargs: Dict):
        self.fn, self.args, self.kwargs = fn, args, kwargs
        sig = inspect.signature(fn)
        self.signal_kwargs = kwargs.copy()
        mapped_args: Dict[str, Any] = dict(zip(sig.parameters.keys(), args))
        if mapped_args:
            self.signal_kwargs.update(mapped_args)

    async def __aenter__(self) -> None:
        await Middleware.emit_signal(f"before_{self.fn.__name__}", self.signal_kwargs)

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type is None:
            await Middleware.emit_signal(f"after_{self.fn.__name__}", self.signal_kwargs)


class MiddlewareWrapper:
    __slots__ = ("fn", "args", "kwargs")

    def __init__(self, fn: Callable):
        self.fn = fn

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> Any:
        stack_frame = traceback.extract_stack(limit=3)[0]
        if (
            stack_frame.filename.endswith("repid/middlewares/wrapper.py")
            and stack_frame.name == "call_decorated_function"
        ):
            # this means that we already have called a middleware
            # so we don't need to call it again
            return await self.call_decorated_function(*args, **kwargs)

        async with MiddlewareWrapperContextManager(self.fn, *args, **kwargs):
            return await self.call_decorated_function(*args, **kwargs)

    async def call_decorated_function(self, *args: Tuple, **kwargs: Dict) -> Any:
        return await self.fn(*args, **kwargs)


class AddMiddleware:
    def __init__(self, cls: Any):
        self.cls = cls

    def __call__(self, *args: Tuple, **kwargs: Dict) -> Any:
        inst = self.cls(*args, **kwargs)
        for key, attr in inspect.getmembers(inst, predicate=inspect.ismethod):
            if callable(attr) and attr.__name__ in available_functions:
                wrapped = MiddlewareWrapper(attr)
                setattr(inst, key, wrapped)
        return inst
