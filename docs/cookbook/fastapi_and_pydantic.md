# FastAPI and Pydantic

Here is how to use Repid with FastAPI and Pydantic.

## FastAPI lifespan to open and close Repid's connection

You can use FastAPI lifespan events with Repid's magic connection mechanism.

```python hl_lines="11"
from contextlib import asynccontextmanager

from fastapi import FastAPI
from repid import Repid, Connection, InMemoryMessageBroker, Job

repid_app = Repid(Connection(InMemoryMessageBroker()))


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with repid_app.magic(auto_disconnect=True):
        yield


app = FastAPI(lifespan=lifespan)


@app.post("/create-repid-job")
async def create_repid_job() -> str:
    routing_key, _, _ = await Job("some_job").enqueue()
    return routing_key.id_
```

## Pydantic models as job arguments

You can pass Pydantic models to Repid's default serializer.

```python
from repid import Router, Job
from pydantic import BaseModel

class MyPydanticModel(BaseModel):
    user_id: int
    actions: list[str]


my_router = Router()

@my_router.actor
async def some_repid_actor(user_id: int, actions: list[str]) -> None:
    await do_something()


# Connection code omitted
await Job(
    "some_repid_actor",
    args=MyPydanticModel(user_id=123, actions=["First action"]),
).enqueue()
```
