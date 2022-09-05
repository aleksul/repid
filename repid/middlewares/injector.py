from __future__ import annotations

import inspect
from functools import update_wrapper
from typing import TypeVar

from . import AVAILABLE_FUNCTIONS, MiddlewareWrapper

ClsT = TypeVar("ClsT")


class InjectMiddleware:
    """A class decorator that wraps methods
    that match AVAILABLE_FUNCTIONS const
    upon creation of the class."""

    def __init__(self, cls: type[ClsT]) -> None:
        update_wrapper(self, cls)
        self.cls = cls

    def __call__(self, *args: tuple, **kwargs: dict) -> ClsT:
        inst = self.cls(*args, **kwargs)
        for key, attr in inspect.getmembers(inst, predicate=inspect.ismethod):
            if callable(attr) and attr.__name__ in AVAILABLE_FUNCTIONS:
                wrapped = MiddlewareWrapper(attr)
                setattr(inst, key, wrapped)
        return inst
