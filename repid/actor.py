from __future__ import annotations

import asyncio
import time
from contextvars import ContextVar
from typing import Any, Callable, Coroutine, NamedTuple, TypeVar

from typing_extensions import ParamSpec

from repid.asyncify import asyncify
from repid.logger import logger
from repid.middlewares import Middleware
from repid.retry_policy import RetryPolicyT, default_retry_policy
from repid.utils import VALID_NAME

FnP = ParamSpec("FnP")
FnR = TypeVar("FnR")


class ActorContext(NamedTuple):
    message_id: str | None = None
    time_limit: int | None = None


class ActorResult(NamedTuple):
    data: Any  # must be encodable
    success: bool
    exception: Exception | None
    started_when: int
    finished_when: int


ActorContexVar = ContextVar("ActorContexVar", default=ActorContext())

DEFAULT_RETRY_POLICY = default_retry_policy()


class Actor:
    """Decorator class. Wraps async and sync functions.
    Logs start and end time of the function, was its execution successful and its return value.
    Allows to specify actor's name and queue.
    """

    __slots__ = ("fn", "asyncified", "name", "queue", "retry_policy")

    def __init__(
        self,
        fn: Callable[FnP, Coroutine[Any, Any, FnR]] | Callable[FnP, FnR],
        name: str | None = None,
        queue: str = "default",
        retry_policy: RetryPolicyT = DEFAULT_RETRY_POLICY,
        run_in_process: bool = False,
    ):

        self.fn = fn
        self.asyncified = asyncify(fn, run_in_process)
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
        self.retry_policy = retry_policy

    async def __call__(self, *args: FnP.args, **kwargs: FnP.kwargs) -> ActorResult:
        ctx = ActorContexVar.get()

        await Middleware.emit_signal(
            "before_actor_run",
            dict(
                actor=self,
                message_id=ctx.message_id,
                args=args,
                kwargs=kwargs,
            ),
        )

        logger_extra = dict(actor=str(self), message_id=ctx.message_id, time_limit=ctx.time_limit)

        result: Any = None
        success: bool
        exception = None

        logger.info("Running {actor} on message {message_id}.", extra=logger_extra)
        logger.debug("Time limit is set to {time_limit}.", extra=logger_extra)

        started_when = time.perf_counter_ns()

        try:
            result = await asyncio.wait_for(
                self.asyncified(*args, **kwargs),
                timeout=ctx.time_limit,
            )
        except Exception as exc:
            exception = exc
            success = False
            logger.exception(
                "Error inside of {actor} on message {message_id}.",
                extra=logger_extra,
            )
        else:
            logger.info(
                "{actor} finished successfully on message {message_id}.",
                extra=logger_extra,
            )
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
                message_id=ctx.message_id,
                args=args,
                kwargs=kwargs,
                result=actor_result,
            ),
        )

        return actor_result

    def __str__(self) -> str:
        return f"Actor({self.fn.__name__}, name='{self.name}', queue='{self.queue}')"
