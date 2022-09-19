from __future__ import annotations

import inspect
from typing import Any, Callable, Coroutine, Dict, List, Protocol, Tuple, TypeVar

import orjson

FnR = TypeVar("FnR", contravariant=True)
Params = Tuple[List, Dict]


class ConverterT(Protocol[FnR]):
    def __init__(self, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> None:
        ...

    def convert_inputs(self, data: str) -> Params:
        ...

    def convert_outputs(self, data: FnR) -> str:
        ...


class DefaultConverter:
    def __init__(self, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> None:
        self.fn = fn
        signature = inspect.signature(fn)
        self.args: dict[str, Any] = {}
        self.kwargs: dict[str, Any] = {}
        self.all_args = False
        self.all_kwargs = False
        for p in signature.parameters.values():
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                self.args.update({p.name: p.default})
            elif (
                p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
                or p.kind == inspect.Parameter.KEYWORD_ONLY
            ):
                self.kwargs.update({p.name: p.default})
            elif p.kind == inspect.Parameter.VAR_POSITIONAL:
                self.all_args = True
            elif p.kind == inspect.Parameter.VAR_KEYWORD:
                self.all_kwargs = True

    def convert_inputs(self, data: str) -> Params:
        if not data:
            return ([], {})
        loaded: dict[str, Any] = orjson.loads(data)
        args = [loaded.pop(name, self.args[name]) for name in self.args]
        kwargs = {name: loaded.pop(name, self.kwargs[name]) for name in self.kwargs}
        if self.all_kwargs:
            kwargs.update(loaded)
        elif self.all_args:
            args.extend(loaded.values())
        return (args, kwargs)

    def convert_outputs(self, data: FnR) -> str:
        return orjson.dumps(data).decode()
