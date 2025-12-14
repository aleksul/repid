from __future__ import annotations

import json
from typing import Annotated

import pytest
from pydantic import BaseModel, ValidationError

from repid import Header
from repid.converter import BasicConverter, ConverterT, PydanticConverter
from repid.data import MessageData
from repid.dependencies import Depends


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


@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header_in_depends(converter: type[ConverterT]) -> None:
    def _header_dep(h: Annotated[str, Header(name="X-Custom")]) -> str:
        return f"got: {h}"

    async def _fn_with_header_depends(d: Annotated[str, Depends(_header_dep)]) -> None: ...

    conv = converter(
        _fn_with_header_depends,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers={"X-Custom": "header_value"},
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: header_value"}


@pytest.mark.parametrize(
    "input_headers",
    [
        {"X-Custom": "header_value"},
        {"X-Other": "whatever", "X-Custom": "header_value"},
    ],
)
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header(converter: type[ConverterT], input_headers: dict) -> None:
    async def _fn_with_header(d: Annotated[str, Header(name="X-Custom")]) -> None: ...

    conv = converter(
        _fn_with_header,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers=input_headers,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "header_value"}


@pytest.mark.parametrize(
    "input_headers",
    [
        None,
        {},
        {"X-Other": "whatever"},
        {"X-Custom": "default_value"},
    ],
)
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header_defaults(converter: type[ConverterT], input_headers: dict) -> None:
    async def _fn_with_header(
        d: Annotated[str, Header(name="X-Custom")] = "default_value",
    ) -> None: ...

    conv = converter(
        _fn_with_header,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers=input_headers,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "default_value"}


@pytest.mark.parametrize(
    "input_payload",
    [
        b'{"h": "default_value"}',
        b'{"h": "default_value", "extra": 123}',
    ],
)
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_payload_in_depends(
    input_payload: bytes,
    converter: type[ConverterT],
) -> None:
    def _dep(h: str) -> str:
        return f"got: {h}"

    async def _fn_with_payload_depends(
        d: Annotated[str, Depends(_dep)],
    ) -> None: ...

    conv = converter(
        _fn_with_payload_depends,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=input_payload,
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: default_value"}


@pytest.mark.parametrize("input_payload", [b"", b"{}", b'{"h": "default_value"}'])
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_payload_in_depends_with_default(
    input_payload: bytes,
    converter: type[ConverterT],
) -> None:
    def _dep_with_default(h: str = "default_value") -> str:
        return f"got: {h}"

    async def _fn_with_payload_depends_default(
        d: Annotated[str, Depends(_dep_with_default)],
    ) -> None: ...

    conv = converter(
        _fn_with_payload_depends_default,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=input_payload,
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: default_value"}


@pytest.mark.parametrize("input_headers", [None, {}, {"X-Other": None}])
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header_in_depends_with_default(
    input_headers: dict | None,
    converter: type[ConverterT],
) -> None:
    def _header_dep_with_default(
        h: Annotated[str, Header(name="X-Custom")] = "default_value",
    ) -> str:
        return f"got: {h}"

    async def _fn_with_header_depends_default(
        d: Annotated[str, Depends(_header_dep_with_default)],
    ) -> None: ...

    conv = converter(
        _fn_with_header_depends_default,
        correlation_id=None,
        fn_locals=locals(),
    )

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers=input_headers,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: default_value"}


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


@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_unsupported_content_type(converter: type[ConverterT]) -> None:
    async def fn(a: int) -> None: ...

    conv = converter(fn, correlation_id=None, fn_locals=None)
    with pytest.raises(ValueError, match="Unsupported content type"):
        await conv.convert_inputs(
            message=MessageData(  # type: ignore[arg-type]
                payload=b"some data",
                headers=None,
                content_type="text/plain",
            ),
            actor=None,  # type: ignore[arg-type]
            server=None,  # type: ignore[arg-type]
            default_serializer=None,  # type: ignore[arg-type]
        )


async def test_basic_converter_payload_not_dict() -> None:
    async def fn(a: int) -> None: ...

    conv = BasicConverter(fn, correlation_id=None, fn_locals=None)
    with pytest.raises(ValueError, match="Payload must be a JSON dict object"):
        await conv.convert_inputs(
            message=MessageData(  # type: ignore[arg-type]
                payload=b"[1, 2, 3]",  # array, not dict
                headers=None,
                content_type="application/json",
            ),
            actor=None,  # type: ignore[arg-type]
            server=None,  # type: ignore[arg-type]
            default_serializer=None,  # type: ignore[arg-type]
        )


async def test_basic_converter_var_kwargs() -> None:
    async def fn(**kwargs: int) -> None: ...

    conv = BasicConverter(fn, correlation_id=None, fn_locals=None)
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b'{"a": 1, "b": 2, "c": 3}',
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"a": 1, "b": 2, "c": 3}


async def test_basic_converter_var_args() -> None:
    async def fn(*args: int) -> None: ...

    conv = BasicConverter(fn, correlation_id=None, fn_locals=None)
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b'{"a": 1, "b": 2}',
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    # For *args, the values (not keys) get extended as args
    assert 1 in args
    assert 2 in args
    assert kwargs == {}


async def test_basic_converter_get_input_schema_raises() -> None:
    async def fn(a: int) -> None: ...

    conv = BasicConverter(fn, correlation_id=None, fn_locals=None)
    with pytest.raises(
        NotImplementedError,
        match="BasicConverter does not support schema generation",
    ):
        conv.get_input_schema()


@pytest.mark.parametrize("payload", [b"", b"{}"])
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_empty_payload(payload: bytes, converter: type[ConverterT]) -> None:
    async def fn(a: int = 5) -> None: ...

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
    assert kwargs == {"a": 5}


@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_positional_only_with_dependency_raises(
    converter: type[ConverterT],
) -> None:
    async def fn(a: Annotated[str, Header()], /) -> None: ...

    with pytest.raises(
        ValueError,
        match="Dependencies in positional-only arguments are not supported",
    ):
        converter(fn, correlation_id=None, fn_locals=None)


async def test_pydantic_converter_var_args_raises() -> None:
    async def fn(*args: int) -> None: ...

    with pytest.raises(ValueError, match=r"\*args and \*\*kwargs are unsupported"):
        PydanticConverter(fn, correlation_id=None, fn_locals=None)


async def test_pydantic_converter_var_kwargs_raises() -> None:
    async def fn(**kwargs: int) -> None: ...

    with pytest.raises(ValueError, match=r"\*args and \*\*kwargs are unsupported"):
        PydanticConverter(fn, correlation_id=None, fn_locals=None)


@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header_no_name_uses_param_name(converter: type[ConverterT]) -> None:
    def _header_dep_no_name(custom_header: Annotated[str, Header()]) -> str:
        return f"got: {custom_header}"

    async def _fn_with_header_no_name(d: Annotated[str, Depends(_header_dep_no_name)]) -> None: ...

    conv = converter(_fn_with_header_no_name, correlation_id=None, fn_locals=locals())

    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers={"custom_header": "header_value"},
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: header_value"}


@pytest.mark.parametrize("input_headers", [None, {}, {"X-Other": None}])
@pytest.mark.parametrize("converter", [BasicConverter, PydanticConverter])
async def test_converter_header_default_uses_param_name(
    input_headers: dict | None,
    converter: type[ConverterT],
) -> None:
    def _header_dep_default_no_name(
        custom_header: Annotated[str, Header()] = "param_default",
    ) -> str:
        return f"got: {custom_header}"

    async def _fn_with_header_default_no_name(
        d: Annotated[str, Depends(_header_dep_default_no_name)],
    ) -> None: ...

    conv = converter(_fn_with_header_default_no_name, correlation_id=None, fn_locals=locals())
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"",
            headers=input_headers,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "got: param_default"}


async def test_pydantic_converter_payload_conflict_type() -> None:
    def _payload_dep_with_a(a: int) -> int:
        return a * 2

    async def _fn_with_conflicting_payload_type(
        a: str,  # type differs from dep
        d: Annotated[int, Depends(_payload_dep_with_a)],
    ) -> None: ...

    with pytest.raises(
        ValueError,
        match="Conflicting field 'a' in Depends and top-level parameters",
    ):
        PydanticConverter(
            _fn_with_conflicting_payload_type,
            correlation_id=None,
            fn_locals=locals(),
        )


async def test_pydantic_converter_payload_conflict_default() -> None:
    def _payload_dep_with_a_default(a: int = 10) -> int:
        return a * 2

    async def _fn_with_conflicting_payload_default(
        d: Annotated[int, Depends(_payload_dep_with_a_default)],
        a: int = 5,  # default differs from dep
    ) -> None: ...

    with pytest.raises(
        ValueError,
        match="Conflicting field 'a' in Depends and top-level parameters",
    ):
        PydanticConverter(
            _fn_with_conflicting_payload_default,
            correlation_id=None,
            fn_locals=locals(),
        )


async def test_pydantic_converter_header_conflict_top_level() -> None:
    async def _fn_with_duplicate_header_conflict(
        h1: Annotated[str, Header(name="X-Same")] = "val1",
        h2: Annotated[int, Header(name="X-Same")] = 1,  # intentionally conflicting type
    ) -> None: ...

    with pytest.raises(
        ValueError,
        match="Conflicting re-declaration of header 'X-Same'",
    ):
        PydanticConverter(
            _fn_with_duplicate_header_conflict,
            correlation_id=None,
            fn_locals=locals(),
        )


async def test_pydantic_converter_header_conflict_in_depends() -> None:
    def _header_dep_same_name_str(
        h: Annotated[str, Header(name="X-Conflict")],
    ) -> str:
        return h

    def _header_dep_same_name_int(
        h: Annotated[int, Header(name="X-Conflict")],
    ) -> int:
        return h

    async def _fn_with_header_depends_conflict(
        d1: Annotated[str, Depends(_header_dep_same_name_str)],
        d2: Annotated[int, Depends(_header_dep_same_name_int)],
    ) -> None: ...

    with pytest.raises(
        ValueError,
        match="Conflicting re-declaration of header 'X-Conflict' in Depends",
    ):
        PydanticConverter(
            _fn_with_header_depends_conflict,
            correlation_id=None,
            fn_locals=locals(),
        )


async def test_pydantic_converter_matching_payload_param_not_duplicated() -> None:
    def _payload_dep_with_matching_a(a: int = 5) -> int:
        return a * 2

    async def _fn_with_matching_payload_param(
        d: Annotated[int, Depends(_payload_dep_with_matching_a)],
        a: int = 5,  # Same type and default as in dep
    ) -> None: ...

    # Should not raise - declarations are matching
    conv = PydanticConverter(
        _fn_with_matching_payload_param,
        correlation_id=None,
        fn_locals=locals(),
    )

    # The payload model should only have 'a' once
    schema = conv.get_input_schema()
    assert schema.payload_schema is not None
    assert schema.payload_schema["properties"]["a"] == {
        "default": 5,
        "title": "A",
        "type": "integer",
    }


async def test_pydantic_converter_matching_header_in_depends_not_duplicated() -> None:
    # Use shared Header instance for matching header
    _shared_header = Header(name="X-Shared")

    def _shared_header_dep(
        h: Annotated[str, _shared_header] = "shared_default",
    ) -> str:
        return h

    async def _fn_with_shared_header_in_depends_and_top_level(
        d: Annotated[str, Depends(_shared_header_dep)],
        h: Annotated[str, _shared_header] = "shared_default",
    ) -> None: ...

    # Should not raise - header declarations are matching
    conv = PydanticConverter(
        _fn_with_shared_header_in_depends_and_top_level,
        correlation_id=None,
        fn_locals=locals(),
    )

    # Should have only one header with name X-Shared
    schema = conv.get_input_schema()
    assert schema.headers_schema is not None
    assert schema.headers_schema["properties"]["X-Shared"] == {
        "default": "shared_default",
        "title": "X-Shared",
        "type": "string",
    }


async def test_pydantic_converter_matching_header_in_multiple_depends_not_duplicated() -> None:
    # Use shared Header instance for matching header
    _shared_header = Header(name="X-Shared")

    def _shared_header_dep(
        h: Annotated[str, _shared_header] = "shared_default",
    ) -> str:
        return h

    def _shared_header_dep2(
        h: Annotated[str, _shared_header] = "shared_default",
    ) -> str:
        return h

    async def _fn_with_shared_header_in_two_depends(
        d1: Annotated[str, Depends(_shared_header_dep)],
        d2: Annotated[str, Depends(_shared_header_dep2)],
    ) -> None: ...

    # Should not raise - header declarations are matching
    conv = PydanticConverter(
        _fn_with_shared_header_in_two_depends,
        correlation_id=None,
        fn_locals=locals(),
    )

    # Should have only one header with name X-Shared
    schema = conv.get_input_schema()
    assert schema.headers_schema is not None
    assert schema.headers_schema["properties"]["X-Shared"] == {
        "default": "shared_default",
        "title": "X-Shared",
        "type": "string",
    }
