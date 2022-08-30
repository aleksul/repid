import asyncio
import logging
from functools import partial
from inspect import getfullargspec, getmembers, isfunction, ismethod
from typing import Any, Callable, Coroutine, Dict, List

from . import POSSIBLE_EVENT_NAMES

logger = logging.getLogger(__name__)


class Middleware:
    events: Dict[str, List[Callable[..., Coroutine]]] = {}

    @classmethod
    def add_event(cls, fn: Callable) -> None:
        name = fn.__name__

        if name not in POSSIBLE_EVENT_NAMES:
            return

        logger.debug(f"Subscribed function '{name}' to middleware signals.")

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
                logger.error(f"Event {fn.__name__} ({fn}) raised exception.", exc_info=True)

        # add wrapped event to the dictionary
        if name not in cls.events:
            cls.events[name] = []
        cls.events[name].append(wrapper)

    @classmethod
    def add_middleware(cls, middleware: Any) -> None:
        middleware_name = middleware.__class__.__name__
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(f"Adding event {middleware_name}.{fn.__name__} to middleware.")
            cls.add_event(fn)

    @classmethod
    async def emit_signal(cls, name: str, kwargs: Dict[str, Any]) -> None:
        if name not in cls.events:
            return
        logger.debug(f"Emitting signal with {name = }.")
        await asyncio.gather(*[fn(**kwargs) for fn in cls.events[name]])
