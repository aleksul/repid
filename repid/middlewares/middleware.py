import asyncio
import inspect
from functools import partial
from typing import Any, Callable, Dict, List, Tuple

import anyio

from repid.asyncify import asyncify

available_functions = (
    "consume",
    "enqueue",
    "queue_declare",
    "queue_flush",
    "queue_delete",
    "ack",
    "nack",
    "requeue",
    "maintenance",
    "get_bucket",
    "store_bucket",
    "delete_bucket",
)


class Middleware:
    _events: Dict[str, List[Callable]] = dict()

    @classmethod
    def add_event(cls, fn: Callable) -> None:
        name = fn.__name__
        # check if name is valid
        if name.startswith(("before_", "after_")) and name.endswith(available_functions):
            # add event to the dictionary
            if name not in cls._events:
                cls._events[name] = list()
            cls._events[name].append(fn)

    @classmethod
    def add_middleware(cls, middleware: Any) -> None:
        for _, fn in inspect.getmembers(middleware, predicate=inspect.ismethod):
            cls.add_event(fn)

    @classmethod
    async def emit_signal(cls, name: str, kwargs: Dict) -> None:
        if name in cls._events:
            async with anyio.create_task_group() as tg:
                for fn in cls._events[name]:
                    argspec = inspect.getargspec(fn)
                    if not asyncio.iscoroutinefunction(fn):
                        fn = asyncify(fn)
                    kwargs = {
                        key: value
                        for key, value in kwargs.items()
                        if key in argspec.args
                        or (key in argspec.defaults if argspec.defaults is not None else False)
                    }
                    tg.start_soon(partial(fn, **kwargs))
