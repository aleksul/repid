from __future__ import annotations

from dataclasses import dataclass, field
from functools import wraps
from time import perf_counter, time
from typing import TYPE_CHECKING, Any, Callable

from repid._utils import FROZEN_DATACLASS, SLOTS_DATACLASS
from repid.connections.abc import BucketBrokerT, ConsumerT, MessageBrokerT
from repid.worker import Worker

if TYPE_CHECKING:
    from repid.data.protocols import RoutingKeyT


@dataclass(**SLOTS_DATACLASS, **FROZEN_DATACLASS)
class EventLog:
    function: str
    args: tuple
    kwargs: dict
    result: Any
    timestamp: float = field(default_factory=time)
    perf_counter: float = field(default_factory=perf_counter)


class EventLogModifier:
    def __init__(self, parent: MessageBrokerT | BucketBrokerT) -> None:
        self._parent = parent
        self.events: list[EventLog] = []
        if isinstance(self._parent, (BucketBrokerT, MessageBrokerT)):
            for method_name in self._parent.__WRAPPED_METHODS__:
                setattr(self._parent, method_name, self.wrapper(getattr(self._parent, method_name)))
            if isinstance(self._parent, MessageBrokerT):
                setattr(  # noqa: B010
                    self._parent,
                    "CONSUMER_CLASS",
                    self.consumer_class_wrapper(self._parent.CONSUMER_CLASS),
                )

    def consumer_class_wrapper(self, class_: type[ConsumerT]) -> type[ConsumerT]:
        class ConsumerWrapper(class_):  # type: ignore[valid-type,misc]
            def __new__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG003
                inst = super().__new__(cls)

                for method in inst.__WRAPPED_METHODS__:
                    setattr(inst, method, self.wrapper(getattr(inst, method)))

                return inst

        return ConsumerWrapper

    def wrapper(self, fn: Callable) -> Callable:
        @wraps(fn)
        async def inner(*args: Any, **kwargs: Any) -> Any:
            result = await fn(*args, **kwargs)
            # log the event
            self.events.append(
                EventLog(
                    function=fn.__name__,
                    args=args,
                    kwargs=kwargs,
                    result=result,
                ),
            )
            return result

        return inner


class RunWorkerOnEnqueueModifier:
    def __init__(self, parent: MessageBrokerT, worker_constructor: Callable[[], Worker]) -> None:
        self._parent = parent
        self._worker_constructor = worker_constructor
        self._topics_by_queue = worker_constructor().topics_by_queue
        self._parent.enqueue = self.wrapper(self._parent.enqueue)  # type: ignore[method-assign]

    def wrapper(self, fn: Callable) -> Callable:
        @wraps(fn)
        async def inner(*args: Any, **kwargs: Any) -> Any:
            result = await fn(*args, **kwargs)

            key: RoutingKeyT = args[0] if len(args) > 0 else kwargs.get("key")

            if key.queue in self._topics_by_queue and key.topic in self._topics_by_queue[key.queue]:
                await self._worker_constructor().run()

            return result

        return inner
