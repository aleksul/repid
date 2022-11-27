import asyncio
import inspect
from functools import partial
from itertools import product
from typing import Any, Callable, Dict, List, Tuple

import anyio

AVAILABLE_FUNCTIONS = (
    "consume",
    "enqueue",
    "queue_declare",
    "queue_flush",
    "queue_delete",
    "ack",
    "nack",
    "reject",
    "get_bucket",
    "store_bucket",
    "delete_bucket",
)

POSSIBLE_EVENT_NAMES = set(
    map(lambda i: i[0] + i[1], product(("before_", "after_"), AVAILABLE_FUNCTIONS))
)


class Middleware:
    _events: Dict[str, List[Callable]] = {}

    @classmethod
    def add_event(cls, fn: Callable) -> None:
        name = fn.__name__
        if name in POSSIBLE_EVENT_NAMES:
            # add event to the dictionary
            if name not in cls._events:
                cls._events[name] = []
            cls._events[name].append(fn)

    @classmethod
    def add_middleware(cls, middleware: Any) -> None:
        for _, fn in inspect.getmembers(middleware, predicate=inspect.ismethod):
            cls.add_event(fn)

    @classmethod
    async def emit_signal(cls, name: str, kwargs: Dict) -> None:
        if name not in cls._events:
            return
        async with anyio.create_task_group() as tg:
            for fn in cls._events[name]:
                argspec = inspect.getfullargspec(fn)
                if not asyncio.iscoroutinefunction(fn):

                    async def wrapper(*args: Tuple, **kwargs: Dict) -> Any:
                        return await anyio.to_thread.run_sync(
                            partial(fn, *args, **kwargs)  # noqa: B023
                        )

                    fn = wrapper
                kwargs = {
                    key: value
                    for key, value in kwargs.items()
                    if key in argspec.args
                    or (key in argspec.defaults if argspec.defaults is not None else False)
                }
                tg.start_soon(partial(fn, **kwargs))
