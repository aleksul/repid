from __future__ import annotations

import inspect
import json
from typing import Any, Callable, Coroutine, Dict, List, Protocol, Tuple, TypeVar
from warnings import warn

from repid._utils import JSON_ENCODER, is_installed

if is_installed("pydantic"):
    from pydantic import BaseModel, Field, create_model

    if is_installed("pydantic", ">=2.0.0a1,<3.0.0"):
        from pydantic import model_serializer, root_validator

        # workaround until AnalysedType releases
        class RootBaseModel(BaseModel):  # pragma: no cover
            @root_validator(pre=True)
            @classmethod
            def populate_root(cls, values: Any) -> dict:
                return {"root": values}

            @model_serializer(mode="wrap")
            def _serialize(self, handler, info):  # type: ignore[no-untyped-def]
                data = handler(self)
                if info.mode == "json":
                    return data["root"]
                return data

            @classmethod
            def model_modify_json_schema(cls, json_schema):  # type: ignore[no-untyped-def]
                return json_schema["properties"]["root"]


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
    def __new__(cls, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> ConverterT[FnR]:  # type: ignore[misc]
        if is_installed("pydantic", ">=2.0.0a1,<3.0.0"):
            return PydanticConverter(fn)
        if is_installed("pydantic", ">=1.0.0,<2.0.0"):
            return PydanticV1Converter(fn)
        return BasicConverter(fn)

    # pretend to be an implementation of ConverterT
    def __init__(self, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> None:
        raise NotImplementedError  # pragma: no cover

    def convert_inputs(self, data: str) -> Params:
        raise NotImplementedError  # pragma: no cover

    def convert_outputs(self, data: FnR) -> str:
        raise NotImplementedError  # pragma: no cover


class BasicConverter:
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
        loaded: dict[str, Any] = json.loads(data)
        args = [loaded.pop(name, self.args[name]) for name in self.args]
        kwargs = {name: loaded.pop(name, self.kwargs[name]) for name in self.kwargs}
        if self.all_kwargs:
            kwargs.update(loaded)
        elif self.all_args:
            args.extend(loaded.values())
        return (args, kwargs)

    def convert_outputs(self, data: FnR) -> str:
        return JSON_ENCODER.encode(data)


class PydanticConverter:
    def __init__(self, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> None:
        self.fn = fn
        signature = inspect.signature(fn)

        self.args: list[str] = []
        self.kwargs: list[str] = []

        for p in signature.parameters.values():
            if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                self.args.append(p.name)
            elif (
                p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
                or p.kind == inspect.Parameter.KEYWORD_ONLY
            ):
                self.kwargs.append(p.name)
            elif (
                p.kind == inspect.Parameter.VAR_POSITIONAL
                or p.kind == inspect.Parameter.VAR_KEYWORD
            ):
                raise ValueError("*args and **kwargs are unsupported")

        self.input_pydantic_model: BaseModel = create_model(  # type: ignore[call-overload]
            f"{fn.__name__}_input_repid_model",
            **{
                p.name: (
                    p.annotation if p.annotation is not inspect.Parameter.empty else Any,
                    p.default if p.annotation is not inspect.Parameter.empty else Field(),
                )
                for p in signature.parameters.values()
            },
        )

        self.validate_output = True
        if (
            signature.return_annotation is inspect.Parameter.empty
            or signature.return_annotation is None
        ):
            self.validate_output = False

        if self.validate_output:
            self.output_type: type = signature.return_annotation
            if not issubclass(self.output_type, BaseModel):
                self.output_pydantic_model = self._generate_output_model(
                    fn.__name__,
                    signature.return_annotation,
                )

    @staticmethod
    def _generate_output_model(fn_name: str, return_annotation: Any) -> BaseModel:
        return create_model(  # type: ignore[return-value]
            f"{fn_name}_output_repid_model",
            __base__=RootBaseModel,
            root=(return_annotation, Field()),
        )

    def convert_inputs(self, data: str) -> Params:
        loaded = dict(self.input_pydantic_model.model_validate_json(data))

        if self.args:
            return ([loaded.pop(arg) for arg in self.args], loaded)

        return ([], loaded)

    def convert_outputs(self, data: FnR) -> str:
        if not self.validate_output:  # there is not type to validate
            return JSON_ENCODER.encode(data)  # fallback to JSON encoding
        if issubclass(self.output_type, BaseModel):
            if isinstance(data, BaseModel):
                return data.model_dump_json()
            return self.output_type.model_validate(data).model_dump_json()
        return self.output_pydantic_model.model_validate(data).model_dump_json()


class PydanticV1Converter(PydanticConverter):  # pragma: no cover
    def __init__(self, fn: Callable[..., Coroutine[Any, Any, FnR]]) -> None:
        super().__init__(fn)
        warn(
            "Pydantic v1 converter will be removed after Pydantic v2 becomes mainstream"
            "and is not included in Repid's test suite.",
            DeprecationWarning,
            stacklevel=2,
        )

    @staticmethod
    def _generate_output_model(fn_name: str, return_annotation: Any) -> BaseModel:
        return create_model(  # type: ignore[return-value]
            f"{fn_name}_output_repid_model",
            __root__=(return_annotation, Field()),
        )

    def convert_inputs(self, data: str) -> Params:
        loaded = dict(self.input_pydantic_model.parse_raw(data))

        if self.args:
            return ([loaded.pop(arg) for arg in self.args], loaded)

        return ([], loaded)

    def convert_outputs(self, data: FnR) -> str:
        if not self.validate_output:  # there is not type to validate
            return JSON_ENCODER.encode(data)  # fallback to JSON encoding
        if issubclass(self.output_type, BaseModel):
            if isinstance(data, BaseModel):
                return data.json()
            return self.output_type.parse_obj(data).json()
        return self.output_pydantic_model.parse_obj(data).json()
