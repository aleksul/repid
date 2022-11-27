from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import partial
from typing import Any, Callable, Dict, NamedTuple, Tuple, Union

import anyio

from repid.utils import VALID_NAME, unix_time


class Result(NamedTuple):
    data: Any
    success: bool
    exception: Union[Exception, None]
    started_when: int
    finished_when: int


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Adds ability to specify actor's name and queue.
    """

    _TIME_LIMIT: ContextVar[Union[int, None]] = ContextVar("time_limit", default=None)

    __slots__ = ("fn", "name", "queue")

    def __init__(
        self,
        fn: Callable,
        name: Union[str, None] = None,
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

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> Result:
        result: Any = None
        success: bool
        started_when = unix_time()
        exception = None
        try:
            with anyio.fail_after(delay=self._TIME_LIMIT.get()):
                if iscoroutinefunction(self.fn):
                    result = await self.fn(*args, **kwargs)
                else:
                    result = await anyio.to_thread.run_sync(partial(self.fn, *args, **kwargs))
        except Exception as exc:
            exception = exc
            success = False
        else:
            success = True

        return Result(
            data=result,
            success=success,
            exception=exception,
            started_when=started_when,
            finished_when=unix_time(),
        )

    def __str__(self) -> str:
        return f"Actor({self.fn.__name__}, name='{self.name}', queue='{self.queue}')"
