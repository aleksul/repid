import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Optional

from .constants import VALID_NAME_RE
from .job import JobResult


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Adds ability to specify actor's name and queue, as well as specify number of retries.
    """

    __slots__ = ("fn", "name", "queue", "retries")

    def __init__(
        self,
        fn: Callable,
        name: Optional[str] = None,
        queue: str = "default",
        retries: int = 1,
    ):
        self.fn = fn
        self.name = name or fn.__name__
        if not VALID_NAME_RE.fullmatch(self.name):
            raise ValueError(
                "Actor name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        self.queue = queue
        if not VALID_NAME_RE.fullmatch(self.queue):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        if retries < 1:
            raise ValueError("Number of retries must be positive integer.")
        self.retries = retries

    async def __call__(self, *args, **kwargs):
        result: Any = None
        success: bool = None
        started_when = int(datetime.utcnow().timestamp())
        loop = asyncio.get_event_loop()
        for _ in range(self.retries):
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
                break
        return JobResult(
            success=success,
            started_when=started_when,
            finished_when=int(datetime.utcnow().timestamp()),
            result=result,
        )

    def __repr__(self):
        return f"Actor({self.fn}, name={self.name}, queue={self.queue}, retries={self.retries})"

    def __str__(self):
        return f"Actor({self.name})"
