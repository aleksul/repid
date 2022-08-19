import logging
from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import partial
from typing import Any, Callable, Dict, NamedTuple, Tuple, Union
from uuid import uuid4

import anyio

from repid.middlewares import Middleware
from repid.utils import VALID_NAME, unix_time

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
        started_when = unix_time()
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
            with anyio.fail_after(delay=time_limit):
                if iscoroutinefunction(self.fn):
                    result = await self.fn(*args, **kwargs)
                else:
                    result = await anyio.to_thread.run_sync(partial(self.fn, *args, **kwargs))
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
            finished_when=unix_time(),
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
