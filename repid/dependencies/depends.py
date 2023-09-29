from __future__ import annotations

from typing import Any, Callable, Coroutine

from repid._asyncify import asyncify


class Depends:
    __repid_dependency__ = True

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], Any | Coroutine], *, run_in_process: bool = False) -> None:
        self.fn = asyncify(fn, run_in_process=run_in_process)

    def override(self, fn: Callable[[], Any | Coroutine], *, run_in_process: bool = False) -> None:
        self.fn = asyncify(fn, run_in_process=run_in_process)
