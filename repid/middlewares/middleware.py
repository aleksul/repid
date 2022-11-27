import asyncio
from functools import partial
from inspect import getfullargspec, getmembers, isfunction, ismethod
from typing import Any, Callable, Coroutine, Dict, List

from repid.logger import logger

from . import POSSIBLE_EVENT_NAMES


class Middleware:
    events: Dict[str, List[Callable[..., Coroutine]]] = {}

    @classmethod
    def add_event(cls, fn: Callable) -> None:
        name = fn.__name__

        if name not in POSSIBLE_EVENT_NAMES:
            return

        logger_extra = dict(fn_name=name, fn=fn)

        logger.debug("Subscribed function '{fn_name}' to middleware signals.", extra=logger_extra)

        is_coroutine = asyncio.iscoroutinefunction(fn)
        argspec = getfullargspec(fn)

        async def wrapper(**kwargs: Dict[str, Any]) -> Any:
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
                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(None, partial(fn, **kwargs))
            # ignore the exception and pass it to the logger
            except Exception:
                logger.exception("Event '{fn_name}' ({fn}) raised exception.", extra=logger_extra)

        # add wrapped event to the dictionary
        if name not in cls.events:
            cls.events[name] = []
        cls.events[name].append(wrapper)

    @classmethod
    def add_middleware(cls, middleware: Any) -> None:
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(
                "Adding event {middleware_name}.{fn_name} to middleware.",
                extra=dict(
                    middleware_name=middleware.__class__.__name__,
                    fn_name=fn.__name__,
                ),
            )
            cls.add_event(fn)

    @classmethod
    async def emit_signal(cls, name: str, kwargs: Dict[str, Any]) -> None:
        if name not in cls.events:
            return
        logger_extra = dict(signal_name=name)
        logger.debug("Emitting signal '{signal_name}'.", extra=logger_extra)
        await asyncio.gather(*[fn(**kwargs) for fn in cls.events[name]])
        logger.debug("Done emitting signal '{signal_name}'.", extra=logger_extra)
