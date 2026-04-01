# Using Pydantic & FastAPI

Repid offers seamless integration with both Pydantic (for data validation) and FastAPI (for web
serving).

## Validating with Pydantic

Repid provides Pydantic validation out-of-the-box. When you define an actor with type hints, Repid
will automatically validate incoming JSON payloads against those types if Pydantic is installed.

### Prerequisite

To ensure correct installation of a supported version, you can run:

```bash
pip install repid[pydantic]
```

Currently, official support targets Pydantic v2 (though v1 may also work).

### Submitting Pydantic Models as Payloads

Repid's default JSON serializer natively supports Pydantic models. You can specify them directly
when sending messages.

```python
from repid import Repid
from pydantic import BaseModel

app = Repid()

class MyPydanticModel(BaseModel):
    user_id: int
    actions: list[str]

# Inside an async function:
await app.send_message_json(
    channel="some_channel",
    payload=MyPydanticModel(
        user_id=123,
        actions=["First action"],
    ),
)
```

You can also nest Pydantic models inside dictionaries or lists, and Repid will automatically extract
them using `.model_dump(mode="json")`.

### Validating Actor Input

As long as Pydantic is installed in your environment, Repid will automatically select the
`PydanticConverter`. This means any type hints you use in your actor signature will be strictly
validated, and you can use `pydantic.Field` for defaults and aliases.

For detailed examples on defining actors with Pydantic validation and using the `FullPayload()` dependency
annotation, see the [Actors guide](../user_guide/actors/parsing.md#parsing-with-pydantic).

## Integration with FastAPI

Because Repid applications manage persistent server connections, you need to ensure the connection
is opened when your web server starts, and closed when it shuts down.

FastAPI's `lifespan` events are perfect for this.

```python hl_lines="11-12"
from contextlib import asynccontextmanager
from fastapi import FastAPI
from repid import Repid, AmqpServer

app = Repid()
app.servers.register_server("default", AmqpServer("amqp://localhost"), is_default=True)

@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    # Open the Repid connection on startup
    async with app.servers.default.connection():
        yield
    # The connection automatically closes when the app shuts down

fastapi_app = FastAPI(lifespan=lifespan)

@fastapi_app.post("/create-job")
async def create_repid_job(data: dict) -> dict:
    # We can safely send messages here because
    # the connection is kept alive by the lifespan
    await app.send_message_json(
        channel="my_background_task",
        payload=data
    )
    return {"status": "ok"}
```
