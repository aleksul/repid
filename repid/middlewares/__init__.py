import asyncio
import logging
from functools import partial
from inspect import getfullargspec, getmembers, isfunction, ismethod
from itertools import product
from typing import Any, Callable, Coroutine, Dict, List, Type

import anyio

logger = logging.getLogger(__name__)

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
    _events: Dict[str, List[Callable[..., Coroutine]]] = {}

    @classmethod
    def add_event(cls, fn: Callable) -> None:
        name = fn.__name__
        if name in POSSIBLE_EVENT_NAMES:
            logger.debug(f"Subscribed function with {name = } to middleware signals.")

            is_coroutine = asyncio.iscoroutinefunction(fn)
            argspec = getfullargspec(fn)

            async def wrapper(**kwargs: Dict) -> Any:
                nonlocal fn, is_coroutine, argspec
                # leave only those kwargs that are in the function signature
                kwargs = {
                    key: value
                    for key, value in kwargs.items()
                    if key in argspec.args
                    or (key in argspec.defaults if argspec.defaults is not None else False)
                }
                # try to call the function
                try:
                    # wrap the function in a coroutine if it is not already
                    if is_coroutine:
                        return await fn(**kwargs)
                    else:
                        return await anyio.to_thread.run_sync(partial(fn, **kwargs))
                # ignore the exception and pass it to the logger
                except Exception as e:
                    logger.error(f"Event {fn.__name__} ({fn}) raised exception: {type(e)}: {e}")

            # add wrapped event to the dictionary
            if name not in cls._events:
                cls._events[name] = []
            cls._events[name].append(wrapper)

    @classmethod
    def add_middleware(cls, middleware: Type[Any]) -> None:
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(
                f"Adding event {middleware.__class__.__name__}.{fn.__name__ = } to middleware."
            )
            cls.add_event(fn)

    @classmethod
    async def emit_signal(cls, name: str, kwargs: Dict) -> None:
        if name not in cls._events:
            return
        logger.debug(f"Emitting signal with {name = }.")
        async with anyio.create_task_group() as tg:
            for fn in cls._events[name]:
                tg.start_soon(partial(fn, **kwargs))
