import asyncio
import json
from random import randint
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel, Field

from repid import Config, Job, PydanticConverter, Router, Worker

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

    j = Job("awesome_job", args={"a": "b", "c": "d"})
    assert not j.args

    with pytest.raises(ValueError, match="Catch me!"):
        Job("awesome_job", args=mymodel)


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_model_args_converter() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    expected = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))
    actual = None

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int = Field(alias="arg3")) -> None:
        nonlocal actual
        actual = MyBaseModel(arg1=arg1, arg2=arg2)

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args={"arg1": expected.arg1, "arg3": expected.arg2})
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert expected == actual


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_return_other_type() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    class MyReturnModel(BaseModel):
        arg3: int

    generated = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int) -> MyReturnModel:
        return {"arg3": arg1, "arg2": arg2}  # type: ignore[return-value]

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args=generated)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert json.loads((await j.result).data) == {"arg3": int(generated.arg1)}  # type: ignore[union-attr]


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_return_appropriate_type() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    class MyReturnModel(BaseModel):
        arg3: int

    generated = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int) -> MyReturnModel:  # noqa: ARG001
        return MyReturnModel(arg3=arg1)  # type: ignore[arg-type]

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args=generated)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert json.loads((await j.result).data) == {"arg3": int(generated.arg1)}  # type: ignore[union-attr]


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_return_simple_type() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    generated = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int) -> int:  # noqa: ARG001
        return int(arg1)

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args=generated)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert (await j.result).data == generated.arg1  # type: ignore[union-attr]


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_model_positional_only_args_converter() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    expected = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))
    actual = None

    @r.actor
    async def my_pydantic_actor(
        arg1: str,
        arg2: int = Field(alias="arg3"),
        /,
    ) -> None:
        nonlocal actual
        actual = MyBaseModel(arg1=arg1, arg2=arg2)

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args={"arg1": expected.arg1, "arg3": expected.arg2})
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert expected == actual


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_model_root() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    expected = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))
    actual = None

    @r.actor
    async def my_pydantic_actor(body: MyBaseModel) -> None:
        nonlocal actual
        actual = body

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args={"body": expected.model_dump(mode="json")})
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert expected == actual


@patch.object(Config, "CONVERTER", PydanticConverter)
async def test_pydantic_return_no_extra_data() -> None:
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    class MyReturnModel(BaseModel):
        arg3: int

    generated = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int) -> MyReturnModel:
        return {"arg2": arg2, "arg3": arg1}  # type: ignore[return-value]

    assert type(r.actors["my_pydantic_actor"].converter) is PydanticConverter

    j = Job("my_pydantic_actor", args=generated)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert json.loads((await j.result).data) == {"arg3": int(generated.arg1)}  # type: ignore[union-attr]
