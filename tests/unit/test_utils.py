import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest
from pydantic import BaseModel

from repid._utils import JSON_ENCODER, asyncify, is_installed

# json encoder tests


class PydanticModel(BaseModel):
    field1: str
    field2: int


@dataclass
class DataclassModel:
    field1: str
    field2: int


@dataclass
class AnotherDataclassModel:
    field1: str
    field2: int
    field3: PydanticModel


@pytest.mark.parametrize(
    ("obj", "expected"),
    [
        pytest.param(
            PydanticModel(field1="hello", field2=123),
            '{"field1":"hello","field2":123}',
            id="pydantic_directly",
        ),
        pytest.param(
            {"example": PydanticModel(field1="hello", field2=123)},
            '{"example":{"field1":"hello","field2":123}}',
            id="pydantic_nested",
        ),
        pytest.param(
            DataclassModel(field1="hello", field2=123),
            '{"field1":"hello","field2":123}',
            id="dataclass_directly",
        ),
        pytest.param(
            {"example": DataclassModel(field1="hello", field2=123)},
            '{"example":{"field1":"hello","field2":123}}',
            id="dataclass_nested",
        ),
        pytest.param(
            AnotherDataclassModel(
                field1="nested",
                field2=456,
                field3=PydanticModel(field1="inner", field2=789),
            ),
            '{"field1":"nested","field2":456,"field3":{"field1":"inner","field2":789}}',
            id="pydantic_inside_dataclass",
        ),
        pytest.param(
            {"example": datetime(2023, 1, 15, 10, 30, 45)},
            '{"example":"2023-01-15T10:30:45"}',
            id="datetime",
        ),
        pytest.param(
            {"example": date(2023, 1, 15)},
            '{"example":"2023-01-15"}',
            id="date",
        ),
        pytest.param(
            {"example": time(10, 30, 45)},
            '{"example":"10:30:45"}',
            id="time",
        ),
        pytest.param(
            {"example": timedelta(days=3, hours=4)},
            f'{{"example":{timedelta(days=3, hours=4).total_seconds()}}}',
            id="timedelta",
        ),
        pytest.param(
            {"example": uuid.UUID("12345678-1234-5678-1234-567812345678")},
            '{"example":"12345678-1234-5678-1234-567812345678"}',
            id="uuid",
        ),
        pytest.param(
            {"example": Decimal("12.34")},
            '{"example":"12.34"}',
            id="decimal",
        ),
    ],
)
def test_json_encoder(obj: object, expected: str) -> None:
    encoded = JSON_ENCODER.encode(obj)
    assert encoded == expected


# is_installed tests


@pytest.mark.parametrize(
    ("dependency", "result"),
    [
        ("pydantic", True),
        ("pytest", True),
        ("flask", False),
        ("blabla", False),
    ],
)
def test_is_imported(dependency: str, result: bool) -> None:
    assert is_installed(dependency) is result


@pytest.mark.parametrize(
    ("dependency", "constraints", "result"),
    [
        ("pydantic", ">=2.0.0,<3.0.0", True),
        ("pydantic", ">=1.0.0", True),
        ("pydantic", ">=3.0.0", False),
        ("pytest", ">=0.0.0,<100.0.0", True),
        ("flask", ">0.0.0,<2.0.0", False),
        ("blabla", ">0.0.0,<2.0.0", False),
    ],
)
def test_is_imported_with_constraints(
    dependency: str,
    constraints: str,
    result: bool,
) -> None:
    assert is_installed(dependency, constraints) is result


@pytest.mark.parametrize(
    ("dependency", "constraints"),
    [
        ("abc", "1.0.0,<2.0"),
        ("abc", "0.0.0"),
    ],
)
def test_is_imported_with_incorrect_constraints(dependency: str, constraints: str) -> None:
    with pytest.raises(ValueError, match="Version constraint must contain an operator"):
        is_installed(dependency, constraints)


# asyncify tests


def sync_function(x: int, y: int) -> int:
    return x + y


async def async_function(x: int, y: int) -> int:
    await asyncio.sleep(0)
    return x * y


async def test_asyncify_sync_function() -> None:
    async_fn = asyncify(sync_function)
    result = await async_fn(5, 3)
    assert result == 8


async def test_asyncify_sync_function_run_in_process() -> None:
    async_fn = asyncify(sync_function, run_in_process=True)
    result = await async_fn(5, 3)
    assert result == 8


async def test_asyncify_already_async_function() -> None:
    async_fn = asyncify(async_function)
    assert async_fn is async_function
    result = await async_fn(5, 3)
    assert result == 15


async def test_asyncify_with_custom_executor() -> None:
    custom_executor = ThreadPoolExecutor(max_workers=2)
    async_fn = asyncify(sync_function, executor=custom_executor)
    result = await async_fn(10, 20)
    assert result == 30
    custom_executor.shutdown(wait=True)
