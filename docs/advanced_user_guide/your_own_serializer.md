# Your own serializer

A serializer in Repid is simply a function that takes an arbitrary Python object
and converts it into `bytes`. These bytes are then sent as the message payload to your broker.

## The `SerializerT` Protocol

The `SerializerT` protocol is very straightforward.
It's a callable that takes `Any` data and returns `bytes`.

```python
from typing import Any
import msgpack

def my_custom_serializer(data: Any) -> bytes:
    # 1. Convert `data` into a storable string or binary format
    # 2. Return the encoded bytes
    return msgpack.packb(data)

# You can also use a class with a `__call__` method:
class MsgPackSerializer:
    def __call__(self, data: Any) -> bytes:
        return msgpack.packb(data)
```

## Using your Custom Serializer

You can configure your custom serializer globally when instantiating
the Repid instance or on a per-message basis.

**Globally:**

```python hl_lines="4"
from repid import Repid

app = Repid(
    default_serializer=my_custom_serializer,
)
```

**Per-Message:**

```python hl_lines="9"
from repid import Repid

app = Repid()

async def main() -> None:
    await app.send_message_json(
        channel="default",
        payload={"example_payload": 123},
        serializer=my_custom_serializer,
    )
```
