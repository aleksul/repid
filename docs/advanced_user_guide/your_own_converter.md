# Your own converter

A converter in Repid is responsible for extracting arguments from a received message
(its payload and headers) and providing them to your actor's underlying function.

By default, Repid provides `DefaultConverter` (which in turn selects either `BasicConverter` or
`PydanticConverter`). However, if you are using a different validation library or
want to define a custom argument injection logic, you can implement your own converter.

## The `ConverterT` Protocol

Your custom converter must implement the `ConverterT` protocol defined in `repid.converter`.

```python
import asyncio
import json
from typing import Callable, Coroutine, Any
from repid.connections.abc import ReceivedMessageT, ServerT
from repid.data import ActorData, CorrelationId, ConverterInputSchema
from repid.serializer import SerializerT

class MyCustomConverter:
    def __init__(
        self,
        fn: Callable[..., Coroutine],
        *,
        fn_locals: dict[str, Any] | None = None,
        correlation_id: CorrelationId | None,
    ) -> None:
        self.fn = fn
        self.correlation_id = correlation_id
        # In a real implementation, you would inspect `fn` to understand what arguments it needs.

    async def convert_inputs(
        self,
        *,
        message: ReceivedMessageT,
        actor: ActorData,
        server: ServerT,
        default_serializer: SerializerT,
    ) -> tuple[list, dict]:
        # 1. Parse the message.payload and message.headers
        # 2. Match the parsed data to the arguments expected by `self.fn`
        # 3. Resolve any dependencies if your framework supports them

        # Example: Simple JSON parser that passes everything as kwargs
        payload_data = json.loads(message.payload) if message.payload else {}

        args = []
        kwargs = payload_data

        return (args, kwargs)

    def get_input_schema(self) -> ConverterInputSchema:
        # Return a schema object used for AsyncAPI documentation generation
        return ConverterInputSchema(
            payload_schema={  # example of JSON schema
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://example.com/product.schema.json",
                "title": "Product",
                "description": "A product in the catalog",
                "type": "object"
            },
            content_type="application/json",
            headers_schema=None,  # or a JSON schema
            correlation_id=self.correlation_id,
        )
```

## Using your Custom Converter

Once your converter is ready, you can pass it to a `Router` or directly
to the `@router.actor` decorator:

```python
from repid import Router

router = Router()

@router.actor(converter=MyCustomConverter)
async def my_actor(data: dict) -> None:
    pass
```
