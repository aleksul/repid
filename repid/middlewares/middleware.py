from __future__ import annotations

import asyncio
from inspect import getfullargspec, getmembers, isfunction, ismethod
from typing import Any, Callable, Coroutine

from repid._asyncify import asyncify
from repid.logger import logger

from . import SUBSCRIBERS_NAMES


class Middleware:
    def __init__(self) -> None:
        self.subscribers: dict[str, list[Callable[..., Coroutine]]] = {}

    def add_subscriber(self, fn: Callable) -> None:
        """Wraps specified callable into an async function with auto kwargs mapping.
        Adds the wrapped function to a list of signal subscribers.

        Args:
            fn (Callable): callable to subscribe. Can be both an async or a normal function.
            Must be named correspondingly to a signal. Shouldn't contain any heavy computation.
        """
        name = fn.__name__

        logger_extra = {"fn_name": name, "fn": fn}

        # check if there is a corresponding signal for the subscriber
        if name not in SUBSCRIBERS_NAMES:
            logger.warning(
                "Function '{fn_name}' wasn't subscribed, as there is no corresponding signal.",
                extra=logger_extra,
            )
            return

        logger.debug(
            "Subscribed function '{fn_name}' to the corresponding middleware signals.",
            extra=logger_extra,
        )

        asyncified = asyncify(fn)
        argspec = getfullargspec(fn)

        async def wrapper(**kwargs: Any) -> Any:
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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Subscriber '{fn_name}' ({fn}) raised an exception.",
                    extra=logger_extra,
                )

        # add wrapped subscriber to the dictionary
        if name not in self.subscribers:
            self.subscribers[name] = []
        self.subscribers[name].append(wrapper)

    def add_middleware(self, middleware: Any) -> None:
        """Scans middleware for functions and tries to subscribe them to signals.

        Args:
            middleware (Any): class or class instance, containing functions or methods,
            which can be subscribed.
        """
        for _, fn in getmembers(middleware, predicate=lambda x: isfunction(x) or ismethod(x)):
            logger.debug(
                "Subscribing '{middleware_name}.{fn_name}' to signals.",
                extra={
                    "middleware_name": middleware.__class__.__name__,
                    "fn_name": fn.__name__,
                },
            )
            self.add_subscriber(fn)

    async def emit_signal(self, name: str, kwargs: dict[str, Any]) -> None:
        """Emit a signal = call every corresponding subscriber.

        Args:
            name (str): name of the signal.
            kwargs (dict[str, Any]): arguments, which were initially supplied to the emitter.
        """
        if name not in self.subscribers:
            return
        logger_extra = {"signal_name": name}
        logger.debug("Emitting signal '{signal_name}'.", extra=logger_extra)
        await asyncio.gather(*[fn(**kwargs) for fn in self.subscribers[name]])
        logger.debug("Done emitting signal '{signal_name}'.", extra=logger_extra)
