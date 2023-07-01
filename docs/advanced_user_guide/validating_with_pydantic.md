# Validating with Pydantic

Repid provides you with Pydantic validation out-of-the box.
Here you will find details on how to use it.

## Prerequisite

Repid will only enable Pydantic validation if Pydantic itself is installed.

To ensure correct installation of a supported version you can run:

```bash
pip install repid[pydantic]
```

Currently official support is targeting Pydantic v2,
while option to go with v1 is also being provided,
however v1 is not being included in the test suite.

## Submitting arguments to a Job

Repid's default serializer supports Pydantic models,
so you can specify them as your Job args.

```python hl_lines="4-6 11-14"
from repid import Router, Job
from pydantic import BaseModel

class MyPydanticModel(BaseModel):
    user_id: int
    actions: list[str]

# connection code is omitted
Job(
    "some_repid_actor",
    args=MyPydanticModel(
        user_id=123,
        actions=["First action"],
    ),
)
```

You can also use Pydantic models inside of another structures
(such as dictionaries, dataclasses, etc.) - Repid's JSON encoder will automatically
convert Pydantic models using `.model_dump(mode="json")`.

```python hl_lines="4-7"
Job(
    "some_another_repid_actor",
    args={
        "body": MyPydanticModel(
            user_id=123,
            actions=["First action"],
        ),
    },
)
```

## Validating Actor input

When running in environment with Pydantic v1 or v2,
Repid will automatically prefer to select appropriate converter.

=== "With Pydantic installed"

    ```python
    from repid import DefaultConverter, BasicConverter, PydanticConverter

    def myfunc(a: int, b: str) -> str:
        return str(a) + b

    c = DefaultConverter(myfunc)  # (1)

    assert type(c) is BasicConverter  # False
    assert type(c) is PydanticConverter  # True
    ```

    1. `DefaultConverter` is just a proxy to create `PydanticConverter` when Pydantic
    is installed and `BasicConverter` otherwise.

=== "Without Pydantic installed"

    ```python
    from repid import DefaultConverter, BasicConverter, PydanticConverter

    def myfunc(a: int, b: str) -> str:
        return str(a) + b

    c = DefaultConverter(myfunc)  # (1)

    assert type(c) is BasicConverter  # True
    assert type(c) is PydanticConverter  # False
    ```

    1. `DefaultConverter` is just a proxy to create `PydanticConverter` when Pydantic
    is installed and `BasicConverter` otherwise.

Considering you have `PydanticConverter` enabled -
all you need to do is create an actor with type hinted arguments.

```python hl_lines="6"
from repid import Router

my_router = Router()

@my_router.actor
async def some_repid_actor(user_id: int, actions: list[str]) -> None:
    ...
```

Those type hints will then be used to create a Pydantic model in the converter,
so that it can validate/transform your arguments,
ensuring that your actor will receive data in the expected format.

You can also assign defaults and `pydantic.Field` to an argument, e.g. to create an alias.

```python hl_lines="8-9"
from repid import Router
from pydantic import Field

my_router = Router()

@my_router.actor
async def some_repid_actor(
    user_id: int = 123,
    actions: list[str] = Field(alias="activity"),
) -> None:
    ...
```

## Validating Actor outputs

If you have Pydantic installed and you have specified return type annotation
(any, other than `None`) - your data will be validated with Pydantic.

```python hl_lines="6-7 15-16 26"
import asyncio
from repid import Router
from pydantic import BaseModel


class MyReturnModel(BaseModel):
    user_id: int

my_router = Router()

@my_router.actor
async def some_repid_actor(
    user_id: int,
    actions: list[str],
) -> MyReturnModel:
    return dict(user_id=user_id, actions=actions)


# connection code is omitted
j = Job(
    "some_repid_actor",
    args=dict(user_id=123, actions=["First action"]),
)
await j.enqueue()
await asyncio.sleep(0.1)
assert (await j.result).data == dict(user_id=123)  # True
```

In this example we can see how Pydantic validation can be useful
to protect sensitive info to be passed outside of the Actor -
result only contains contents, specified in the `MyReturnModel`.

## Disabling Pydantic Converter

To disable Pydantic converter, simply set Config to use BasicConverter.

```python
from repid import Config, BasicConverter

Config.CONVERTER = BasicConverter
```
