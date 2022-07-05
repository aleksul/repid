from asyncio import iscoroutinefunction
from functools import partial
from typing import Any, Callable, Dict, Optional, Tuple

import anyio

from repid.data import ResultBucket
from repid.utils import VALID_NAME, unix_time


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Adds ability to specify actor's name and queue.
    """

    __slots__ = ("fn", "name", "queue")

    def __init__(
        self,
        fn: Callable,
        name: Optional[str] = None,
        queue: str = "default",
    ):
        self.fn = fn
        self.name = name or fn.__name__
        if not VALID_NAME.fullmatch(self.name):
            raise ValueError(
                "Actor name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        self.queue = queue
        if not VALID_NAME.fullmatch(self.queue):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> ResultBucket:
        result: Any = None
        success: bool
        started_when = unix_time()
        try:
            if iscoroutinefunction(self.fn):
                result = await self.fn(*args, **kwargs)
            else:
                await anyio.to_thread.run_sync(partial(self.fn, *args, **kwargs), cancellable=True)
        except Exception as exc:
            exception = exc
            success = False
        else:
            success = True
        timestamp = unix_time()
        return ResultBucket(
            id_="",
            data=result,
            success=success,
            exception=exception or None,
            started_when=started_when,
            finished_when=timestamp,
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return f"Actor({self.fn}, {self.name=}, {self.queue=})"

    def __str__(self) -> str:
        return f"Actor({self.name})"
