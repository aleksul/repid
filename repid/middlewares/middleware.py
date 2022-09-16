from __future__ import annotations

import asyncio
from inspect import getfullargspec, getmembers, isfunction, ismethod
from typing import Any, Callable, Coroutine

from repid._asyncify import asyncify
from repid.logger import logger

from . import POSSIBLE_EVENT_NAMES


class Middleware:
    def __init__(self) -> None:
        self.events: dict[str, list[Callable[..., Coroutine]]] = {}

    def add_event(self, fn: Callable) -> None:
        name = fn.__name__

        if name not in POSSIBLE_EVENT_NAMES:
            return

        logger_extra = dict(fn_name=name, fn=fn)

        logger.debug("Subscribed function '{fn_name}' to middleware signals.", extra=logger_extra)

        asyncified = asyncify(fn)
        argspec = getfullargspec(fn)

        async def wrapper(**kwargs: dict[str, Any]) -> Any:
            # leave only those kwargs that are in the function signature
            kwargs = {
                key: value
                for key, value in kwargs.items()
                if key in argspec.args
                or (key in argspec.defaults if argspec.defaults is not None else False)
            }
            # try to call the function
            try:
                return await asyncified(**kwargs)
            # ignore the exception and pass it to the logger
            except Exception:
                logger.exception("Event '{fn_name}' ({fn}) raised exception.", extra=logger_extra)

        # add wrapped event to the dictionary
        if name not in self.events:
            self.events[name] = []
        self.events[name].append(wrapper)

    def add_middleware(self, middleware: Any) -> None:
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(
                "Adding event {middleware_name}.{fn_name} to middleware.",
                extra=dict(
                    middleware_name=middleware.__class__.__name__,
                    fn_name=fn.__name__,
                ),
            )
            self.add_event(fn)

    async def emit_signal(self, name: str, kwargs: dict[str, Any]) -> None:
        if name not in self.events:
            return
        logger_extra = dict(signal_name=name)
        logger.debug("Emitting signal '{signal_name}'.", extra=logger_extra)
        await asyncio.gather(*[fn(**kwargs) for fn in self.events[name]])
        logger.debug("Done emitting signal '{signal_name}'.", extra=logger_extra)
