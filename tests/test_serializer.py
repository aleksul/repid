from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from repid.job import Job

pytestmark = pytest.mark.usefixtures("fake_connection")


def test_args_serialization() -> None:
    j = Job("my-awesome-job", args={"random": "string"})
    assert j.args == '{"random":"string"}'


def test_dataclass_args_serialization() -> None:
    @dataclass
    class MyData:
        a: int
        b: float
        c: datetime
        d: timedelta

    j = Job(
        "my-awesome-job",
        args=MyData(
            a=123,
            b=567.34,
            c=datetime(2020, 1, 1, 12, 0, 0),
            d=timedelta(days=2, hours=3),
        ),
    )
    assert j.args == '{"a":123,"b":567.34,"c":"2020-01-01T12:00:00","d":183600.0}'
