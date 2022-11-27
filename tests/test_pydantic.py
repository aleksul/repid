import asyncio
from random import randint

from pydantic import BaseModel

from repid import Job, Router, Worker


async def test_pydantic_model_args(fake_connection):
    r = Router()

    class MyBaseModel(BaseModel):
        arg1: str
        arg2: int

    expected = MyBaseModel(arg1=str(randint(0, 1000)), arg2=randint(0, 1000))
    actual = None

    @r.actor
    async def my_pydantic_actor(arg1: str, arg2: int):
        nonlocal actual
        actual = MyBaseModel(arg1=arg1, arg2=arg2)

    j = Job("my_pydantic_actor", args=expected)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(routers=[r], messages_limit=1)

    await asyncio.wait_for(myworker.run(), 5.0)

    assert expected == actual
