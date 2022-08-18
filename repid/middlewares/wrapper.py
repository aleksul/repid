import inspect
import traceback
from types import TracebackType
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, Tuple, Type, TypeVar, Union

from . import AVAILABLE_FUNCTIONS, Middleware

if TYPE_CHECKING:
    from repid.connection import Bucketing, Messaging

FnReturnT = TypeVar("FnReturnT")


class MiddlewareWrapperContextManager:
    """An async context manager that emits signals before and after the function call."""

    __slots__ = ("fn", "signal_kwargs")

    def __init__(self, fn: Callable[..., Awaitable[FnReturnT]], args: Tuple, kwargs: Dict):
        self.fn = fn
        self.signal_kwargs = kwargs.copy()
        args_names = inspect.signature(fn).parameters.keys()
        self.signal_kwargs.update(zip(args_names, args))

    async def __aenter__(self) -> None:
        await Middleware.emit_signal(f"before_{self.fn.__name__}", self.signal_kwargs)

    async def __aexit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_val: Union[BaseException, None],
        exc_tb: Union[TracebackType, None],
    ) -> None:
        if exc_type is None:
            await Middleware.emit_signal(f"after_{self.fn.__name__}", self.signal_kwargs)


class MiddlewareWrapper:
    """A function decorator that wraps the function with middleware context manager."""

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[..., Awaitable[FnReturnT]]) -> None:
        self.fn = fn

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> FnReturnT:
        stack_frame = traceback.extract_stack(limit=3)[0]
        if (
            stack_frame.filename.endswith("repid/middlewares/wrapper.py")
            and stack_frame.name == "call_decorated_function"
        ):
            # this means that we already have called a middleware
            # so we don't need to call it again
            return await self.call_decorated_function(*args, **kwargs)

        async with MiddlewareWrapperContextManager(self.fn, args, kwargs):
            return await self.call_decorated_function(*args, **kwargs)

    async def call_decorated_function(self, *args: Tuple, **kwargs: Dict) -> FnReturnT:
        return await self.fn(*args, **kwargs)


class InjectMiddleware:
    """A class decorator that wraps certain methods upon creation of the class."""

    __slots__ = ("cls",)

    def __init__(self, cls: Type[Union["Messaging", "Bucketing"]]) -> None:
        self.cls = cls

    def __call__(self, *args: Tuple, **kwargs: Dict) -> Union["Messaging", "Bucketing"]:
        inst = self.cls(*args, **kwargs)  # type: ignore[arg-type]
        for key, attr in inspect.getmembers(inst, predicate=inspect.ismethod):
            if callable(attr) and attr.__name__ in AVAILABLE_FUNCTIONS:
                wrapped = MiddlewareWrapper(attr)
                setattr(inst, key, wrapped)
        return inst
