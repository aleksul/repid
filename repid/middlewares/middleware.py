from typing import Any, Callable, Coroutine, Dict, List, Literal, get_args

import anyio

Events = Literal[
    "before_consume",
    "after_consume",
    "before_enqueue",
    "after_enqueue",
    "before_queue_declare",
    "after_queue_declare",
    "before_queue_flush",
    "after_queue_flush",
    "before_queue_delete",
    "after_queue_delete",
    "before_message_ack",
    "after_message_ack",
    "before_message_nack",
    "after_message_nack",
    "before_message_requeue",
    "after_message_requeue",
    "before_maintenance",
    "after_maintenance",
    "before_get_bucket",
    "after_get_bucket",
    "before_store_bucket",
    "after_store_bucket",
    "before_delete_bucket",
    "after_delete_bucket",
]


class _Middleware:
    __possible_events = get_args(Events)

    def __new__(cls):  # Singleton
        if not hasattr(cls, "__instance"):
            cls.__instance = cls.__init__()
        return cls.__instance

    def __init__(self):
        self._events: Dict[Events, List[Callable[..., Coroutine[Any, Any, Any]]]] = dict()

    def _add_event(self, name: Events, fn: Callable[..., Coroutine[Any, Any, Any]]) -> None:
        if name not in self._events:
            self._events[name] = list()
        self._events[name].append(fn)

    def add_middleware(self, middleware: Any) -> None:
        for event in dir(middleware):
            if event in self.__possible_events:
                self._add_event(event, getattr(middleware, event))  # type: ignore[arg-type]

    async def emit_signal(self, name: Events, *data: List[Any]) -> None:
        if name in self._events:
            async with anyio.create_task_group() as tg:
                for fn in self._events[name]:
                    tg.start_soon(fn, *data[: fn.__code__.co_argcount])


Middleware = _Middleware()


def with_middleware(fn):
    async def wrapper(*args, **kwargs):
        nonlocal fn
        await Middleware.emit_signal(f"before_{fn.__name__}", args)
        result = await fn(*args, **kwargs)
        await Middleware.emit_signal(f"after_{fn.__name__}", args)
        return result

    return wrapper
