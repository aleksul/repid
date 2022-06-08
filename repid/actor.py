import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional

from repid.data import ResultBucket
from repid.job import JobResult
from repid.utils import VALID_NAME, unix_time


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Adds ability to specify actor's name and queue, as well as specify number of retries.
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

    async def __call__(self, *args, **kwargs):
        result: Any = None
        success: bool
        started_when = unix_time()
        loop = asyncio.get_event_loop()
        try:
            if asyncio.iscoroutinefunction(self.fn):
                result = await self.fn(*args, **kwargs)
            else:
                with ThreadPoolExecutor() as pool:
                    result = await loop.run_in_executor(
                        pool,
                        functools.partial(self.fn, *args, **kwargs),
                    )
        except Exception as exc:
            result = f"Exception occured while running the job: {type(exc)}: {exc}."
            success = False
        else:
            success = True
        return ResultBucket(
            id=
            data=result,
            success=success,
            started_when=started_when,
            finished_when=unix_time(),
            
        )

    def __repr__(self):
        return f"Actor({self.fn}, {self.name=}, {self.queue=})"

    def __str__(self):
        return f"Actor({self.name})"
