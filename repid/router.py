from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Callable, NamedTuple, TypeVar

from repid._asyncify import asyncify
from repid.actor import ActorData
from repid.converter import DefaultConverter
from repid.retry_policy import default_retry_policy
from repid.utils import VALID_NAME

if TYPE_CHECKING:
    from repid.converter import ConverterT
    from repid.retry_policy import RetryPolicyT

YourFunc = TypeVar("YourFunc", bound=Callable)


class RouterDefaults(NamedTuple):
    queue: str = "default"
    retry_policy: RetryPolicyT = default_retry_policy()
    run_in_process: bool = False
    converter: type[ConverterT] = DefaultConverter


class Router:
    __slots__ = ("actors", "defaults")

    def __init__(self, *, defaults: RouterDefaults | None = None) -> None:
        self.actors: dict[str, ActorData] = dict()
        self.defaults = defaults or RouterDefaults()

    @property
    def topics(self) -> frozenset[str]:
        return frozenset(self.actors.keys())

    @property
    def queues(self) -> frozenset[str]:
        return frozenset(q.queue for q in self.actors.values())

    @property
    def topics_by_queue(self) -> dict[str, frozenset[str]]:
        topics = self.topics
        return {q: frozenset(t for t in topics if self.actors[t].queue == q) for q in self.queues}

    def actor(
        self,
        fn: YourFunc | None = None,
        /,
        name: str | None = None,
        queue: str | None = None,
        retry_policy: RetryPolicyT | None = None,
        run_in_process: bool | None = None,
        converter: type[ConverterT] | None = None,
    ) -> YourFunc | Callable[[YourFunc], YourFunc]:
        """Actor decorator.

        Args:
            name (str | None, optional):
                actor's name.
                Used for routing a message to this actor (message.topic == actor.name).
                Defaults to the name of your wrapped function.
            queue (str | None, optional):
                queue that actor will receive messages from.
                Defaults to `Router.defaults.queue`.
            retry_policy (RetryPolicyT | None, optional):
                decides for how long we should delay retry of a message.
                Defaults to `Router.defaults.retry_policy`.
            run_in_process (bool | None, optional):
                If True, runs synchronous actors inside of a `ProcessPoolExecutor`
                instead of the default `ThreadPoolExecutor`.
                Defaults to `Router.defaults.run_in_process`.
            converter (type[ConverterT] | None, optional):
                Class that decides how the arguments and return type
                should be validated & parsed.
                Defaults to `Router.defaults.converter`.

        Returns:
            YourFunc: your initial function.
        """

        if fn is None:
            return partial(  # type: ignore[return-value]
                self.actor,
                name=name,
                queue=queue,
                retry_policy=retry_policy,
                run_in_process=run_in_process,
                converter=converter,
            )

        a = ActorData(
            fn=asyncify(fn, run_in_process=run_in_process or self.defaults.run_in_process),
            name=name or fn.__name__,
            queue=queue or self.defaults.queue,
            retry_policy=retry_policy or self.defaults.retry_policy,
            converter=self.defaults.converter(fn) if converter is None else converter(fn),
        )

        if not VALID_NAME.fullmatch(a.name):
            raise ValueError(
                "Actor name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        if not VALID_NAME.fullmatch(a.queue):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )

        self.actors[a.name] = a
        return fn
