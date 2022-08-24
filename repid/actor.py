import asyncio
import logging
import time
from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import partial
from typing import Any, Callable, Dict, NamedTuple, Tuple, Union
from uuid import uuid4

from repid.middlewares import Middleware
from repid.utils import VALID_NAME

logger = logging.getLogger(__name__)


class ActorResult(NamedTuple):
    data: Any  # must be encodable
    success: bool
    exception: Union[Exception, None]
    started_when: int
    finished_when: int


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Allows to specify actor's name and queue.
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

    async def __call__(self, *args: Tuple, **kwargs: Dict) -> ActorResult:
        result: Any = None
        success: bool
        started_when = time.perf_counter_ns()
        exception = None
        time_limit = self._TIME_LIMIT.get()
        run_id = uuid4().hex
        logger_extra = dict(run_id=run_id)
        logger.info(f"Running {str(self)}.", extra=logger_extra)
        logger.debug(f"Time limit is set to {time_limit}.", extra=logger_extra)
        await Middleware.emit_signal(
            "before_actor_run",
            dict(
                actor=self,
                run_id=run_id,
                args=args,
                kwargs=kwargs,
            ),
        )
        try:
            if iscoroutinefunction(self.fn):
                await asyncio.wait_for(self.fn(*args, **kwargs), timeout=time_limit)
            else:
                loop = asyncio.get_running_loop()
                future = loop.run_in_executor(None, partial(self.fn, *args, **kwargs))
                await asyncio.wait_for(future, timeout=time_limit)
        except Exception as exc:
            exception = exc
            success = False
            logger.error(
                f"Error occured while running {str(self)}.",
                extra=logger_extra,
                exc_info=True,
            )
        else:
            logger.info(f"{str(self)} finished successfully.", extra=logger_extra)
            success = True

        actor_result = ActorResult(
            data=result,
            success=success,
            exception=exception,
            started_when=started_when,
            finished_when=time.perf_counter_ns(),
        )

        await Middleware.emit_signal(
            "after_actor_run",
            dict(
                actor=self,
                run_id=run_id,
                args=args,
                kwargs=kwargs,
                result=actor_result,
            ),
        )

        return actor_result

    def __str__(self) -> str:
        return f"Actor({self.fn.__name__}, name='{self.name}', queue='{self.queue}')"
