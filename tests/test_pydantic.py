import asyncio
from random import randint
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from repid import Job, Router, Worker
from repid.config import Config

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_pydantic_model_args() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    expected = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))
    actual = None

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int) -> None:
        nonlocal actual
        actual = MyBaseModel(arg1=arg1, arg2=arg2)

    j = Job("my_pydantic_actor", args=expected)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert expected == actual


def new_serializer(data: Any) -> str:
    if isinstance(data, BaseModel):
        raise ValueError("Catch me!")
    return ""


@patch.object(Config, "SERIALIZER", new_serializer)
async def test_wrong_serializer() -> None:
    assert Config.SERIALIZER is new_serializer

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    mymodel = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))

    j = Job("awesome_job", args=dict(a="b", c="d"))
    assert j.args == ""

    with pytest.raises(ValueError, match="Catch me!"):
        Job("awesome_job", args=mymodel)
