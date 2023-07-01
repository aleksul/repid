from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel

from repid._utils import JSON_ENCODER


def test_pydantic_in_dict() -> None:
    class MyModel(BaseModel):
        field1: str
        field2: int

    encoded = JSON_ENCODER.encode(
        {
            "example": MyModel(field1="hello", field2=123),
        },
    )

    assert encoded == '{"example":{"field1":"hello","field2":123}}'


def test_nested_pydantic() -> None:
    class MyModel(BaseModel):
        field1: str
        field2: int

    class MySecondModel(BaseModel):
        field3: str
        field4: MyModel

    encoded = JSON_ENCODER.encode(
        {
            "example": MySecondModel(field3="world", field4=MyModel(field1="hello", field2=123)),
        },
    )

    assert encoded == '{"example":{"field3":"world","field4":{"field1":"hello","field2":123}}}'


def test_dataclass() -> None:
    @dataclass
    class MyModel:
        field1: str
        field2: int

    encoded = JSON_ENCODER.encode(
        {
            "example": MyModel(field1="hello", field2=123),
        },
    )

    assert encoded == '{"example":{"field1":"hello","field2":123}}'


def test_dataclass_with_pydantic() -> None:
    class MyModel(BaseModel):
        field1: str
        field2: int

    @dataclass
    class MySecondModel:
        field3: str
        field4: MyModel

    encoded = JSON_ENCODER.encode(
        {
            "example": MySecondModel(field3="world", field4=MyModel(field1="hello", field2=123)),
        },
    )

    assert encoded == '{"example":{"field3":"world","field4":{"field1":"hello","field2":123}}}'


@given(time_=st.one_of(st.dates(), st.datetimes(), st.times()))
def test_times(time_: date | time | datetime) -> None:
    encoded = JSON_ENCODER.encode(
        {
            "example": time_,
        },
    )

    assert encoded == f'{{"example":"{time_.isoformat()}"}}'


@given(timedelta_=st.timedeltas())
def test_timedeltas(timedelta_: timedelta) -> None:
    encoded = JSON_ENCODER.encode(
        {
            "example": timedelta_,
        },
    )

    assert encoded == f'{{"example":{timedelta_.total_seconds()}}}'
