# Payload & Headers Parsing

When a worker pulls a message from the broker, it receives a raw byte payload and a dictionary of
string headers. Repid automatically parses this data and maps it to your actor function's arguments.

How Repid performs this parsing depends on whether you have **Pydantic** installed in your
environment.

## Parsing without Pydantic

If you run Repid in a vanilla Python environment without Pydantic, Repid uses a lightweight
`BasicConverter` under the hood.

1. **Payload Decoding**: Repid uses the configured `default_serializer` (which defaults to standard
   `json`) to decode the incoming byte payload into a Python dictionary.
2. **Argument Matching**: Repid inspects your actor function's signature. If the keys in the decoded
   JSON dictionary match the names of your function arguments, the values are passed in directly.

```python
# Assuming payload: {"user_id": 123, "is_active": true}
@router.actor
async def process_user(user_id, is_active):
    # user_id will be an int (123)
    # is_active will be a bool (True)
    pass
```

!!! warning "No Type Validation"
    Without Pydantic, Repid **does not validate or coerce types**. If
    the sender passes `{"user_id": "123"}` (a string),
    your function will receive a string, even if you
    typed it as `user_id: int`.

## Parsing with Pydantic

If you install Pydantic (`pip install repid[pydantic]` or just `pip install pydantic>=2.0.0`), Repid
automatically upgrades its internal parsing engine to use the `PydanticConverter`.

This enables strict type validation, automatic type coercion, and AsyncAPI schema generation out of
the box!

```python
# Assuming payload: {"user_id": "123", "is_active": 1}
@router.actor
async def process_user(user_id: int, is_active: bool):
    # user_id is automatically coerced to an int (123)
    # is_active is automatically coerced to a bool (True)
    pass
```

Because Repid relies directly on Pydantic's powerful validation engine, you can use all of
Pydantic's advanced typing features directly in your actor's function signature:

```python
from typing import Annotated
from pydantic import BaseModel, Field
from annotated_types import Gt
import uuid

class UserAddress(BaseModel):
    city: str
    country: str

@router.actor
async def process_order(
    # Use annotated-types for simple constraints
    quantity: Annotated[int, Gt(0)],

    # Use Pydantic's Field for complex defaults (like UUIDs or timestamps)
    order_id: Annotated[str, Field(default_factory=lambda: uuid.uuid4().hex)],

    # Use nested Pydantic models for complex nested payload data
    shipping_address: UserAddress,
):
    # Repid guarantees that if the actor executes, `quantity` > 0,
    # `order_id` is auto-generated if missing,
    # and `shipping_address` is strictly formatted!
    pass
```

### The `FullPayload()` Annotation

Sometimes you don't want your JSON payload's keys scattered as individual arguments. If you want to
accept an entire Pydantic model representing the exact root JSON payload, you can use the `FullPayload`
annotation:

```python
from typing import Annotated
from pydantic import BaseModel
from repid import FullPayload

class UserPayload(BaseModel):
    user_id: int
    is_active: bool

@router.actor
async def process_user(user: Annotated[UserPayload, FullPayload()]):
    # `user` contains the fully validated payload model
    print(user.user_id)
```

!!! tip
    `FullPayload()` is only available when Pydantic is installed.

## Extracting Headers

Headers are metadata attached to a message (like `topic`, `correlation_id`, or custom tracking
tags). You can extract specific headers directly into your actor arguments using the `Header`
dependency injection.

```python
from typing import Annotated
from repid import Header

@router.actor
async def my_actor(
    payload_data: str,
    correlation_id: Annotated[str, Header(alias="correlation-id")],
    custom_trace: Annotated[str | None, Header()] = None
):
    print(f"Tracking: {correlation_id} / {custom_trace}")
```

If Pydantic is installed, headers extracted this way are also strongly validated and coerced! If a
header is required (no default value) but is missing from the message, Pydantic will raise a
validation error.

## Explicitly Specifying the Converter

By default, Repid automatically selects the converter based on whether Pydantic is available in your
environment (i.e. `DefaultConverter`). However, you can explicitly specify which converter to use
when creating your router or actor.

To use the lightweight `BasicConverter` even when Pydantic is installed:

```python
from repid import Router, BasicConverter

router = Router(converter=BasicConverter)  # (1)

@router.actor
async def my_actor(user_id: int, is_active: bool):
    # Uses BasicConverter - no type validation or coercion
    pass
```

1. First option - override via Router

To explicitly use the `PydanticConverter`:

```python
from repid import Router, PydanticConverter

router = Router()


@router.actor(converter=PydanticConverter)  # (1)
async def my_actor(user_id: int, is_active: bool):
    # Uses PydanticConverter - full type validation and coercion
    pass
```

1. Second option - override direcly on the actor

This is useful when you want to ensure consistent behavior across different environments or when you
have specific performance or validation requirements.

!!! tip
    You can also implement custom converters to define your own parsing logic.
    This allows you to integrate alternative validation frameworks,
    add custom serialization support, or optimize for
    specific use cases in your application.
