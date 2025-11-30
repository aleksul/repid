from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, ValidationError

from repid.converter import BasicConverter, ConverterT, PydanticConverter
from repid.data import MessageData


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(b'{"a": 5, "b": 3}', {"a": 5, "b": 3}, id="full_dict"),
        pytest.param(b'{"a": 5}', {"a": 5, "b": 1}, id="partial_dict"),
    ],
)
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_parses_payload_args_kwargs(
    converter: type[ConverterT],
    payload: bytes,
    expected: dict,
) -> None:
    async def fn(a: int, b: int = 1) -> None: ...

    conv = converter(fn, correlation_id=None, fn_locals=None)
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=payload,
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == expected


@pytest.mark.parametrize(
    ("payload", "expected_args", "expected_kwargs"),
    [
        pytest.param(b'{"b": 3, "a": 5}', [5], {"b": 3}, id="full_dict"),
        pytest.param(b'{"a": 5}', [5], {"b": 1}, id="partial_dict"),
    ],
)
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_parses_payload_positional_args(
    converter: type[ConverterT],
    payload: bytes,
    expected_args: list,
    expected_kwargs: dict,
) -> None:
    async def fn(a: int, /, b: int = 1) -> None: ...

    conv = converter(fn, correlation_id=None, fn_locals=None)
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=payload,
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == expected_args
    assert kwargs == expected_kwargs


async def test_pydantic_converter_properly_coerces_types() -> None:
    class MyModel(BaseModel):
        a: int
        b: float
        c: str

    async def fn(model: MyModel, d: int) -> None: ...

    conv = PydanticConverter(
        fn,
        fn_locals=locals(),  # we need to pass locals as we have locally defined MyModel
        correlation_id=None,
    )
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=json.dumps(
                {
                    "model": {
                        "a": 10.0,  # wrong type, should be coerced
                        "b": "3.14",  # wrong type, should be coerced
                        "c": "test",
                    },
                    "d": "42",  # wrong type, should be coerced
                },
            ).encode(),
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {
        "model": MyModel(
            a=10,
            b=3.14,
            c="test",
        ),
        "d": 42,
    }


@pytest.mark.parametrize(
    ("payload", "expected_error_substring"),
    [
        pytest.param(
            b'{"a": "not_an_int", "b": 3}',
            "Input should be a valid integer",
            id="invalid_int",
        ),
        pytest.param(
            b'{"a": 5, "b": "not_a_float"}',
            "Input should be a valid number",
            id="invalid_float",
        ),
        pytest.param(
            b'{"a": 5}',
            "Field required",
            id="missing_field",
        ),
    ],
)
async def test_pydantic_converter_validation_errors(
    payload: bytes,
    expected_error_substring: str,
) -> None:
    async def fn(a: int, b: float) -> None: ...

    conv = PydanticConverter(fn, fn_locals=None, correlation_id=None)
    with pytest.raises(ValidationError, match=expected_error_substring):
        await conv.convert_inputs(
            message=MessageData(  # type: ignore[arg-type]
                payload=payload,
                headers=None,
                content_type="application/json",
            ),
            actor=None,  # type: ignore[arg-type]
            server=None,  # type: ignore[arg-type]
            default_serializer=None,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_not_json_error(converter: type[ConverterT]) -> None:
    async def fn(a: int, b: float) -> None: ...

    conv = converter(fn, correlation_id=None, fn_locals=None)
    with pytest.raises(ValueError, match="Unsupported content type"):
        await conv.convert_inputs(
            message=MessageData(  # type: ignore[arg-type]
                payload=b"<body>This is not a json!</body>",
                headers=None,
                content_type="application/xml",
            ),
            actor=None,  # type: ignore[arg-type]
            server=None,  # type: ignore[arg-type]
            default_serializer=None,  # type: ignore[arg-type]
        )


async def test_pydantic_converter_generates_input_schema() -> None:
    class MyModel(BaseModel):
        a: int
        b: float

    async def fn(model: MyModel, d: str = "default") -> None: ...

    conv = PydanticConverter(
        fn,
        fn_locals=locals(),  # we need to pass locals as we have locally defined MyModel
        correlation_id=None,
    )
    schema = conv.get_input_schema()
    assert schema.payload_schema == {
        "$defs": {
            "MyModel": {
                "properties": {
                    "a": {"title": "A", "type": "integer"},
                    "b": {"title": "B", "type": "number"},
                },
                "required": ["a", "b"],
                "title": "MyModel",
                "type": "object",
            },
        },
        "properties": {
            "model": {"$ref": "#/components/schemas/MyModel"},
            "d": {"default": "default", "title": "D", "type": "string"},
        },
        "required": ["model"],
        "title": "fn_payload",
        "type": "object",
    }
    assert schema.headers_schema is None
    assert schema.content_type == "application/json"
    assert schema.examples is None
