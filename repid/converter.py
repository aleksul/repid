from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Protocol

from repid._utils import get_dependency, is_installed
from repid.data import ConverterInputSchema

if is_installed("pydantic"):
    from pydantic import BaseModel, Field, create_model

if TYPE_CHECKING:
    from repid.dependencies.protocols import DependencyT

Params = tuple[list, dict]


class ConverterT(Protocol):
    def __init__(self, fn: Callable[..., Coroutine]) -> None: ...

    def convert_inputs(self, data: bytes, content_type: str | None) -> Params: ...

    @property
    def dependencies(self) -> dict[str, DependencyT]: ...

    def get_input_schema(self) -> ConverterInputSchema: ...


class DefaultConverter:
    def __new__(cls, fn: Callable[..., Coroutine]) -> ConverterT:  # type: ignore[misc]
        if is_installed("pydantic", ">=2.0.0,<3.0.0"):
            return PydanticConverter(fn)
        if is_installed("pydantic"):
            raise ValueError("Unsupported Pydantic version, only 2.x is supported.")
        return BasicConverter(fn)

    # pretend to be an implementation of ConverterT
    def __init__(self, fn: Callable[..., Coroutine]) -> None:
        raise NotImplementedError  # pragma: no cover

    def convert_inputs(self, data: bytes, content_type: str | None) -> Params:
        raise NotImplementedError  # pragma: no cover

    @property
    def dependencies(self) -> dict[str, DependencyT]:
        raise NotImplementedError  # pragma: no cover

    def get_input_schema(self) -> ConverterInputSchema:
        raise NotImplementedError  # pragma: no cover


class BasicConverter:
    def __init__(self, fn: Callable[..., Coroutine]) -> None:
        self.fn = fn
        self.signature = inspect.signature(fn)
        self.args: dict[str, Any] = {}
        self.kwargs: dict[str, Any] = {}
        self.dependency_kwargs: dict[str, DependencyT] = {}
        self.all_args = False
        self.all_kwargs = False
        for p in self.signature.parameters.values():
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                if get_dependency(p.annotation) is not None:
                    raise ValueError("Dependencies in positional-only arguments are not supported.")
                self.args[p.name] = p.default
            elif p.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                if (dep := get_dependency(p.annotation)) is not None:
                    self.dependency_kwargs[p.name] = dep
                    continue
                self.kwargs[p.name] = p.default
            elif p.kind == inspect.Parameter.VAR_POSITIONAL:
                self.all_args = True
            elif p.kind == inspect.Parameter.VAR_KEYWORD:
                self.all_kwargs = True

    def convert_inputs(self, data: bytes, content_type: str | None) -> Params:
        if not data:
            return ([], {})
        if content_type not in (None, "application/json"):
            raise ValueError(f"Unsupported content type: {content_type}")
        loaded: dict[str, Any] = json.loads(data)
        args = [loaded.pop(name, self.args[name]) for name in self.args]
        kwargs = {name: loaded.pop(name, self.kwargs[name]) for name in self.kwargs}
        if self.all_kwargs:
            kwargs.update(loaded)
        elif self.all_args:
            args.extend(loaded.values())
        return (args, kwargs)

    @property
    def dependencies(self) -> dict[str, DependencyT]:
        return self.dependency_kwargs

    def get_input_schema(self) -> ConverterInputSchema:
        raise NotImplementedError("BasicConverter does not support schema generation.")


class PydanticConverter:
    def __init__(self, fn: Callable[..., Coroutine]) -> None:
        self.fn = fn
        signature = inspect.signature(fn)

        self.args: list[str] = []
        self.kwargs: list[str] = []
        self.dependency_kwargs: dict[str, DependencyT] = {}

        for p in signature.parameters.values():
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                if get_dependency(p.annotation) is not None:
                    raise ValueError("Dependencies in positional-only arguments are not supported.")
                self.args.append(p.name)
            elif p.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                if (dep := get_dependency(p.annotation)) is not None:
                    self.dependency_kwargs[p.name] = dep
                    continue
                self.kwargs.append(p.name)
            elif p.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                raise ValueError("*args and **kwargs are unsupported")

        self.input_pydantic_model: BaseModel = create_model(  # type: ignore[call-overload]
            f"{fn.__name__}_input_repid_model",
            **{
                p.name: (
                    p.annotation if p.annotation is not inspect.Parameter.empty else Any,
                    p.default if p.default is not inspect.Parameter.empty else Field(),
                )
                for p in signature.parameters.values()
                if p.name not in self.dependency_kwargs
            },
        )

    def convert_inputs(self, data: bytes, content_type: str | None) -> Params:
        if not data:
            return ([], {})

        if content_type not in (None, "application/json"):
            raise ValueError(f"Unsupported content type: {content_type}")

        loaded = dict(self.input_pydantic_model.model_validate_json(data))

        if self.args:
            return ([loaded.pop(arg) for arg in self.args], loaded)

        return ([], loaded)

    @property
    def dependencies(self) -> dict[str, DependencyT]:
        return self.dependency_kwargs

    def get_input_schema(self) -> ConverterInputSchema:
        return ConverterInputSchema(
            payload_schema=self.input_pydantic_model.model_json_schema(),
            content_type="application/json",
        )
