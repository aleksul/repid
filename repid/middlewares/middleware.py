import asyncio
import logging
from contextvars import ContextVar
from functools import partial
from inspect import getfullargspec, getmembers, isfunction, ismethod
from typing import Any, Callable, Coroutine, Dict, List, Type, Union

import anyio

from . import POSSIBLE_EVENT_NAMES

logger = logging.getLogger(__name__)

ALL_MIDDLEWARES: ContextVar[Union[List["Middleware"], None]] = ContextVar(
    "ALL_MIDDLEWARES", default=None
)


class Middleware:
    def __new__(cls: Type["Middleware"]) -> "Middleware":
        inst = super().__new__(cls)
        all_middlewares = ALL_MIDDLEWARES.get()
        if all_middlewares is None:
            ALL_MIDDLEWARES.set([inst])
        else:
            all_middlewares.append(inst)
            ALL_MIDDLEWARES.set(all_middlewares)
        return inst

    def __init__(self) -> None:
        self._events: Dict[str, List[Callable[..., Coroutine]]] = {}

    def add_event(self, fn: Callable) -> None:
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
            if name not in self._events:
                self._events[name] = []
            self._events[name].append(wrapper)

    def add_middleware(self, middleware: Type[Any]) -> None:
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(
                f"Adding event {middleware.__class__.__name__}.{fn.__name__ = } to middleware."
            )
            self.add_event(fn)

    async def emit_signal(self, name: str, kwargs: Dict) -> None:
        if name not in self._events:
            return
        logger.debug(f"Emitting signal with {name = }.")
        async with anyio.create_task_group() as tg:
            for fn in self._events[name]:
                tg.start_soon(partial(fn, **kwargs))


async def emit_signal_all(name: str, kwargs: Dict) -> None:
    middlewares = ALL_MIDDLEWARES.get()
    if middlewares is None:
        return
    async with anyio.create_task_group() as tg:
        for middleware in middlewares:
            tg.start_soon(partial(middleware.emit_signal, name, kwargs))
