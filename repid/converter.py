from __future__ import annotations

import asyncio
import inspect
import itertools
import json
from collections.abc import Callable, Coroutine, Iterable
from typing import TYPE_CHECKING, Any, Protocol, cast

from repid._utils import is_installed
from repid.data import ConverterInputSchema
from repid.dependencies._utils import DependencyContext, get_dependency
from repid.dependencies.depends import Depends as DependsClass
from repid.dependencies.header_dependency import Header

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT, ServerT
    from repid.data import ActorData, CorrelationId
    from repid.dependencies._utils import DependencyT
    from repid.serializer import SerializerT

if is_installed("pydantic"):
    from pydantic import BaseModel, Field, create_model
    from pydantic.fields import FieldInfo


FnParams = tuple[list, dict]


async def _resolve_dependencies(
    message: ReceivedMessageT,
    actor: ActorData,
    server: ServerT,
    default_serializer: SerializerT,
    parsed_args: list[Any],
    parsed_kwargs: dict[str, Any],
    parsed_headers: dict[str, Any],
    headers_id_to_name: dict[int, str],
    dependencies: dict[str, DependencyT],
) -> dict[str, Any]:
    context = DependencyContext(
        message=message,
        actor=actor,
        server=server,
        default_serializer=default_serializer,
        parsed_args=parsed_args,
        parsed_kwargs=parsed_kwargs,
        parsed_headers=parsed_headers,
        headers_id_to_name=headers_id_to_name,
    )

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
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> None: ...

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
        server: ServerT,
        default_serializer: SerializerT,
    ) -> FnParams: ...

    def get_input_schema(self) -> ConverterInputSchema: ...


class DefaultConverter:
    def __new__(  # type: ignore[misc]
        cls,
        fn: Callable[..., Coroutine],
        *,
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> ConverterT:
        if is_installed("pydantic", ">=2.0.0,<3.0.0"):
            return PydanticConverter(fn, fn_locals=fn_locals, correlation_id=correlation_id)
        if is_installed("pydantic"):
            raise ValueError("Unsupported Pydantic version, only 2.x is supported.")
        return BasicConverter(fn, fn_locals=fn_locals, correlation_id=correlation_id)

    # pretend to be an implementation of ConverterT
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
        server: ServerT,
        default_serializer: SerializerT,
    ) -> FnParams:
        raise NotImplementedError  # pragma: no cover

    def get_input_schema(self) -> ConverterInputSchema:
        raise NotImplementedError  # pragma: no cover


class BasicConverter:
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> None:
        self.fn = fn
        self.correlation_id = correlation_id
        local = fn_locals.copy() if fn_locals is not None else {}
        local.update(fn.__globals__)
        signature = inspect.signature(
            fn,
            eval_str=True,
            locals=local,
            globals=fn.__globals__,
        )
        self.args: dict[str, Any] = {}
        self.kwargs: dict[str, Any] = {}
        self.dependency_kwargs: dict[str, DependencyT] = {}
        self.all_args = False
        self.all_kwargs = False
        for p in signature.parameters.values():
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
        self.headers_id_to_name = self._build_headers_id_to_name_mapping(
            signature,
            self.dependency_kwargs,
        )
        self.header_defaults = self._build_header_defaults(
            signature,
            self.dependency_kwargs,
        )

    @staticmethod
    def _build_headers_id_to_name_mapping(
        signature: inspect.Signature,
        dependency_kwargs: dict[str, DependencyT],
    ) -> dict[int, str]:
        headers_id_to_name: dict[int, str] = {}
        for dep_arg_name, dep in dependency_kwargs.items():
            if isinstance(dep, Header):
                headers_id_to_name[id(dep)] = dep._name or signature.parameters[dep_arg_name].name
            elif isinstance(dep, DependsClass):
                for header_dep, param in dep._iter_header_arguments():
                    headers_id_to_name[id(header_dep)] = header_dep._name or param.name
        return headers_id_to_name

    @staticmethod
    def _build_header_defaults(
        signature: inspect.Signature,
        dependency_kwargs: dict[str, DependencyT],
    ) -> dict[str, Any]:
        defaults: dict[str, Any] = {}
        for dep_arg_name, dep in dependency_kwargs.items():
            if isinstance(dep, Header):
                param = signature.parameters[dep_arg_name]
                header_name = dep._name or param.name
                if param.default is not inspect.Parameter.empty:
                    defaults[header_name] = param.default
            elif isinstance(dep, DependsClass):
                for header_dep, param in dep._iter_header_arguments():
                    header_name = header_dep._name or param.name
                    if param.default is not inspect.Parameter.empty:
                        defaults[header_name] = param.default
        return defaults

    def _parse_payload(self, message: ReceivedMessageT) -> dict[str, Any]:
        if not message.payload:
            return {}
        if message.content_type not in (None, "", "application/json"):
            raise ValueError(f"Unsupported content type: {message.content_type}")
        parsed = json.loads(message.payload)
        if not isinstance(parsed, dict):
            raise ValueError("Payload must be a JSON dict object.")
        return cast(dict[str, Any], parsed)

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
        server: ServerT,
        default_serializer: SerializerT,
    ) -> FnParams:
        loaded = self._parse_payload(message)

        args = [loaded.pop(name, self.args[name]) for name in self.args]
        kwargs = {name: loaded.pop(name, self.kwargs[name]) for name in self.kwargs}
        if self.all_kwargs:
            kwargs.update(loaded)
        elif self.all_args:
            args.extend(loaded.values())

        resolved = await _resolve_dependencies(
            message=message,
            actor=actor,
            server=server,
            default_serializer=default_serializer,
            parsed_args=args,
            parsed_kwargs={**kwargs, **loaded},
            parsed_headers={**self.header_defaults, **(message.headers or {})},
            headers_id_to_name=self.headers_id_to_name,
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
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> None:
        self.fn = fn
        self.correlation_id = correlation_id
        local = fn_locals.copy() if fn_locals is not None else {}
        local.update(fn.__globals__)
        signature = inspect.signature(
            fn,
            eval_str=True,
            locals=local,
            globals=fn.__globals__,
        )

        self.args, self.kwargs, self.dependency_kwargs = self._parse_signature(signature)

        self.payload_pydantic_model = self._build_payload_model(
            f"{fn.__name__}_payload",
            signature,
            self.dependency_kwargs,
        )
        self.headers_pydantic_model = self._build_headers_model(
            f"{fn.__name__}_headers",
            signature,
            self.dependency_kwargs,
        )
        self.headers_id_to_name = self._build_headers_id_to_name_mapping(self.dependency_kwargs)

    @staticmethod
    def _parse_signature(
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
                if (dep := get_dependency(p.annotation)) is not None:
                    dependency_kwargs[p.name] = dep
                else:
                    kwargs.append(p.name)
        return args, kwargs, dependency_kwargs

    @staticmethod
    def _build_payload_model(
        model_name: str,
        signature: inspect.Signature,
        dependency_kwargs: dict[str, DependencyT],
    ) -> type[BaseModel] | None:
        fields: dict[str, tuple[Any, Any]] = {
            p.name: (
                p.annotation if p.annotation is not inspect.Parameter.empty else Any,
                p.default if p.default is not inspect.Parameter.empty else Field(),
            )
            for p in signature.parameters.values()
            if p.name not in dependency_kwargs
        }

        dependencies: Iterable[DependsClass] = filter(
            lambda d: isinstance(d, DependsClass),  # type: ignore[arg-type]
            dependency_kwargs.values(),
        )

        # Include params from Depends
        for param in itertools.chain.from_iterable(
            d._iter_payload_arguments() for d in dependencies
        ):
            if param.name in fields:
                # check if field is declared the same way as in top-level
                if (
                    (
                        param.annotation is not inspect.Parameter.empty
                        and fields[param.name][0] != param.annotation
                    )
                    or (
                        param.annotation is inspect.Parameter.empty
                        and fields[param.name][0] is not Any
                    )
                    or (
                        param.default is not inspect.Parameter.empty
                        and fields[param.name][1] != param.default
                    )
                    or (
                        param.default is inspect.Parameter.empty
                        and not isinstance(fields[param.name][1], FieldInfo)
                    )
                ):
                    raise ValueError(
                        f"Conflicting field '{param.name}' in Depends and top-level parameters.",
                    )
                # declaration matches, so we can just skip the parameter
                continue
            fields[param.name] = (
                param.annotation if param.annotation is not inspect.Parameter.empty else Any,
                param.default if param.default is not inspect.Parameter.empty else Field(),
            )

        if not fields:
            return None

        return create_model(model_name, **fields)  # type: ignore[call-overload, no-any-return]

    @staticmethod
    def _build_headers_model(
        model_name: str,
        signature: inspect.Signature,
        dependency_kwargs: dict[str, DependencyT],
    ) -> type[BaseModel] | None:
        header_fields: dict[str, tuple[Any, Any]] = {}
        for name, dependency in dependency_kwargs.items():
            if isinstance(dependency, Header):
                param = signature.parameters[name]
                header_name = dependency._name or name

                if header_name in header_fields:
                    # check if header is already declared
                    if (
                        (
                            param.annotation is not inspect.Parameter.empty
                            and header_fields[header_name][0] != param.annotation
                        )
                        or (
                            param.annotation is inspect.Parameter.empty
                            and header_fields[header_name][0] is not Any
                        )
                        or (
                            param.default is not inspect.Parameter.empty
                            and header_fields[header_name][1] != param.default
                        )
                        or (
                            param.default is inspect.Parameter.empty
                            and not isinstance(header_fields[header_name][1], FieldInfo)
                        )
                    ):
                        raise ValueError(
                            f"Conflicting re-declaration of header '{header_name}'.",
                        )
                    # declaration matches, so we can just skip the header
                    continue

                header_fields[header_name] = (
                    param.annotation if param.annotation is not inspect.Parameter.empty else Any,
                    param.default if param.default is not inspect.Parameter.empty else Field(),
                )
            if isinstance(dependency, DependsClass):
                for (
                    header_dependency,
                    header_parameter,
                ) in dependency._iter_header_arguments():
                    header_name = header_dependency._name or header_parameter.name

                    if header_name in header_fields:
                        # check if header is already declared
                        if (
                            (
                                header_parameter.annotation is not inspect.Parameter.empty
                                and header_fields[header_name][0] != header_parameter.annotation
                            )
                            or (
                                header_parameter.annotation is inspect.Parameter.empty
                                and header_fields[header_name][0] is not Any
                            )
                            or (
                                header_parameter.default is not inspect.Parameter.empty
                                and header_fields[header_name][1] != header_parameter.default
                            )
                            or (
                                header_parameter.default is inspect.Parameter.empty
                                and not isinstance(header_fields[header_name][1], FieldInfo)
                            )
                        ):
                            raise ValueError(
                                f"Conflicting re-declaration of header '{header_name}' in Depends.",
                            )
                        # declaration matches, so we can just skip the header
                        continue

                    header_fields[header_name] = (
                        header_parameter.annotation
                        if header_parameter.annotation is not inspect.Parameter.empty
                        else Any,
                        header_parameter.default
                        if header_parameter.default is not inspect.Parameter.empty
                        else Field(),
                    )

        if not header_fields:
            return None

        return create_model(model_name, **header_fields)  # type: ignore[call-overload, no-any-return]

    @staticmethod
    def _build_headers_id_to_name_mapping(
        dependency_kwargs: dict[str, DependencyT],
    ) -> dict[int, str]:
        headers_id_to_name: dict[int, str] = {}
        for name, dependency in dependency_kwargs.items():
            if isinstance(dependency, Header):
                headers_id_to_name[id(dependency)] = dependency._name or name
            elif isinstance(dependency, DependsClass):
                for header_dep, param in dependency._iter_header_arguments():
                    headers_id_to_name[id(header_dep)] = header_dep._name or param.name
        return headers_id_to_name

    def _parse_payload(self, message: ReceivedMessageT) -> BaseModel | None:
        if self.payload_pydantic_model is None:
            return None
        if message.content_type not in (None, "", "application/json"):
            raise ValueError(f"Unsupported content type: {message.content_type}")
        return self.payload_pydantic_model.model_validate_json(message.payload or b"{}")

    def _parse_headers(self, message: ReceivedMessageT) -> BaseModel | None:
        if self.headers_pydantic_model is None:
            return None
        return self.headers_pydantic_model.model_validate(message.headers or {})

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
        server: ServerT,
        default_serializer: SerializerT,
    ) -> FnParams:
        validated_payload = self._parse_payload(message)
        loaded = (
            {
                name: getattr(validated_payload, name)
                for name in self.payload_pydantic_model.model_fields
            }
            if validated_payload is not None and self.payload_pydantic_model is not None
            else {}
        )
        if self.args:  # if there are positional args - pop them from loaded
            args: list[Any] = [loaded.pop(arg, None) for arg in self.args]
        else:
            args = []
        validated_headers = self._parse_headers(message)
        parsed_headers = (
            {
                name: getattr(validated_headers, name)
                for name in self.headers_pydantic_model.model_fields
            }
            if validated_headers is not None and self.headers_pydantic_model is not None
            else {}
        )
        resolved = await _resolve_dependencies(
            message=message,
            actor=actor,
            server=server,
            default_serializer=default_serializer,
            parsed_args=args,
            parsed_kwargs=loaded,
            parsed_headers=parsed_headers,
            headers_id_to_name=self.headers_id_to_name,
            dependencies=self.dependency_kwargs,
        )
        kwargs = {name: value for name, value in loaded.items() if name in self.kwargs}
        return (args, {**kwargs, **resolved})

    def get_input_schema(self) -> ConverterInputSchema:
        return ConverterInputSchema(
            payload_schema=(
                self.payload_pydantic_model.model_json_schema(
                    ref_template="#/components/schemas/{model}",
                )
                if self.payload_pydantic_model
                else None
            ),
            content_type="application/json" if self.payload_pydantic_model else "",
            headers_schema=(
                self.headers_pydantic_model.model_json_schema(
                    ref_template="#/components/schemas/{model}",
                )
                if self.headers_pydantic_model
                else None
            ),
            correlation_id=self.correlation_id,
        )
