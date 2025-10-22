from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Coroutine
from typing import Annotated, Any, Protocol, get_args, get_origin

from repid._utils import is_installed
from repid.connections.abc import ReceivedMessageT
from repid.data import ActorData, ConverterInputSchema, CorrelationId
from repid.dependencies._utils import DependencyContext, DependencyT, get_dependency
from repid.dependencies.header_dependency import Header

if is_installed("pydantic"):
    from pydantic import Field, create_model


FnParams = tuple[list, dict]


async def _resolve_dependencies(
    message: ReceivedMessageT,
    actor: ActorData,
    parsed_headers: dict[int, Any],
    dependencies: dict[str, DependencyT],
) -> dict[str, Any]:
    context = DependencyContext(message=message, actor=actor, parsed_headers=parsed_headers)

    unresolved_dependencies: dict[str, Coroutine] = {
        dep_name: dep.resolve(context=context) for dep_name, dep in dependencies.items()
    }

    unresolved_dependencies_names, unresolved_dependencies_values = (
        unresolved_dependencies.keys(),
        unresolved_dependencies.values(),
    )

    resolved = await asyncio.gather(*unresolved_dependencies_values)

    return dict(zip(unresolved_dependencies_names, resolved, strict=False))


class ConverterT(Protocol):
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        correlation_id: CorrelationId | None,
    ) -> None: ...

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> FnParams: ...

    def get_input_schema(self) -> ConverterInputSchema: ...


class DefaultConverter:
    def __new__(  # type: ignore[misc]
        cls,
        fn: Callable[..., Coroutine],
        *,
        correlation_id: CorrelationId | None,
    ) -> ConverterT:
        if is_installed("pydantic", ">=2.0.0,<3.0.0"):
            return PydanticConverter(fn, correlation_id=correlation_id)
        if is_installed("pydantic"):
            raise ValueError("Unsupported Pydantic version, only 2.x is supported.")
        return BasicConverter(fn, correlation_id=correlation_id)

    # pretend to be an implementation of ConverterT
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        correlation_id: CorrelationId | None,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> FnParams:
        raise NotImplementedError  # pragma: no cover

    def get_input_schema(self) -> ConverterInputSchema:
        raise NotImplementedError  # pragma: no cover


class BasicConverter:
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        correlation_id: CorrelationId | None,
    ) -> None:
        self.fn = fn
        self.correlation_id = correlation_id
        self.signature = inspect.signature(fn)
        self.args: dict[str, Any] = {}
        self.kwargs: dict[str, Any] = {}
        self.dependency_kwargs: dict[str, DependencyT] = {}
        self._param_by_name: dict[str, inspect.Parameter] = {}
        self.all_args = False
        self.all_kwargs = False
        for p in self.signature.parameters.values():
            self._param_by_name[p.name] = p
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

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> FnParams:
        # parse payload
        payload_bytes = message.payload
        content_type = message.content_type

        loaded: dict[str, Any] = {}
        if payload_bytes:
            if content_type not in (None, "application/json"):
                raise ValueError(f"Unsupported content type: {content_type}")
            loaded = json.loads(payload_bytes)

        # map payload to function parameters
        args = [loaded.pop(name, self.args[name]) for name in self.args]
        kwargs = {name: loaded.pop(name, self.kwargs[name]) for name in self.kwargs}
        if self.all_kwargs:
            kwargs.update(loaded)
        elif self.all_args:
            args.extend(loaded.values())

        # decode headers for header dependencies (no typing coercion here)
        raw_headers: dict[str, str] = message.headers or {}
        parsed_headers: dict[int, Any] = {}
        for dep_arg_name, dep in self.dependency_kwargs.items():
            if isinstance(dep, Header):
                header_param = self._param_by_name[dep_arg_name]
                header_name = dep._name or header_param.name
                parsed_headers[id(dep)] = raw_headers.get(header_name)

        # resolve dependencies (including headers, message, and Depends)
        resolved = await _resolve_dependencies(
            message=message,
            actor=actor,
            parsed_headers=parsed_headers,
            dependencies=self.dependency_kwargs,
        )

        kwargs.update(resolved)
        return (args, kwargs)

    def get_input_schema(self) -> ConverterInputSchema:
        raise NotImplementedError("BasicConverter does not support schema generation.")


class PydanticConverter:
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        correlation_id: CorrelationId | None,
    ) -> None:
        self.fn = fn
        self.correlation_id = correlation_id
        signature = inspect.signature(fn)

        self.args, self.kwargs, self.dependency_kwargs = self._parse_signature(signature)
        self._param_by_name: dict[str, inspect.Parameter] = {
            p.name: p for p in signature.parameters.values()
        }

        self.input_pydantic_model = self._build_input_model(signature)
        self.headers_pydantic_model = self._build_headers_model()

    def _parse_signature(
        self,
        signature: inspect.Signature,
    ) -> tuple[list[str], list[str], dict[str, DependencyT]]:
        args: list[str] = []
        kwargs: list[str] = []
        dependency_kwargs: dict[str, DependencyT] = {}
        for p in signature.parameters.values():
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                if get_dependency(p.annotation) is not None:
                    raise ValueError("Dependencies in positional-only arguments are not supported.")
                args.append(p.name)
                continue
            if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                raise ValueError("*args and **kwargs are unsupported")
            if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                dep = get_dependency(p.annotation)
                if dep is not None:
                    dependency_kwargs[p.name] = dep
                else:
                    kwargs.append(p.name)
        return args, kwargs, dependency_kwargs

    def _build_input_model(self, signature: inspect.Signature) -> Any:
        return create_model(  # type: ignore[call-overload]
            f"{self.fn.__name__}_input_repid_model",
            **{
                p.name: (
                    p.annotation if p.annotation is not inspect.Parameter.empty else Any,
                    p.default if p.default is not inspect.Parameter.empty else Field(),
                )
                for p in signature.parameters.values()
                if p.name not in self.dependency_kwargs
            },
        )

    def _unwrap_annotated(self, ann: Any) -> Any:
        if get_origin(ann) is Annotated:
            args = get_args(ann)
            return args[0] if args else Any
        return ann

    def _build_headers_model(self) -> Any | None:
        header_fields: dict[str, tuple[Any, Any]] = {}
        for name, dep in self.dependency_kwargs.items():
            if isinstance(dep, Header):
                p = self._param_by_name[name]
                base_type = self._unwrap_annotated(
                    p.annotation if p.annotation is not inspect.Parameter.empty else Any,
                )
                alias = dep._name or name
                if p.default is inspect.Parameter.empty:
                    field_def = Field(..., alias=alias)
                else:
                    field_def = Field(p.default, alias=alias)
                header_fields[name] = (base_type, field_def)

        if not header_fields:
            return None

        return create_model(  # type: ignore[call-overload]
            f"{self.fn.__name__}_headers_repid_model",
            **header_fields,
        )

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
    ) -> FnParams:
        # parse payload
        payload_bytes = message.payload
        content_type = message.content_type

        loaded: dict[str, Any] = {}
        if payload_bytes:
            if content_type not in (None, "application/json"):
                raise ValueError(f"Unsupported content type: {content_type}")
            # payload is bytes -> validate from JSON
            loaded = dict(self.input_pydantic_model.model_validate_json(payload_bytes))

        # map payload to function parameters
        if self.args:
            args: list[Any] = [loaded.pop(arg, None) for arg in self.args]
            kwargs: dict[str, Any] = loaded
        else:
            args = []
            kwargs = loaded

        # decode and validate headers for header dependencies
        parsed_headers: dict[int, Any] = {}
        if self.headers_pydantic_model is not None:
            raw_headers: dict[str, str] = message.headers or {}
            headers_obj = self.headers_pydantic_model.model_validate(raw_headers)
            for name, dep in self.dependency_kwargs.items():
                if isinstance(dep, Header):
                    parsed_headers[id(dep)] = getattr(headers_obj, name)

        # resolve dependencies and merge into kwargs
        resolved = await _resolve_dependencies(
            message=message,
            actor=actor,
            parsed_headers=parsed_headers,
            dependencies=self.dependency_kwargs,
        )
        kwargs.update(resolved)

        return (args, kwargs)

    def get_input_schema(self) -> ConverterInputSchema:
        return ConverterInputSchema(
            payload_schema=self.input_pydantic_model.model_json_schema(),
            content_type="application/json",
            headers_schema=(
                self.headers_pydantic_model.model_json_schema()
                if self.headers_pydantic_model
                else None
            ),
            correlation_id=self.correlation_id,
        )
