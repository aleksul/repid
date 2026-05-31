<!-- markdownlint-disable MD033 -->
# Repid

<p align="center">
  <a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
    <img alt="Repid's logo" width=400 src="https://gist.github.com/aleksul/fedbe168f1fc59c5aac3ddd17ecff30a/raw/b9467303f55517d99633d6551de223cd6534b149/repid_logo_borders.svg">
  </a>
</p>

<p align="center">
<b>Repid</b> is a simple, fast, and extensible async task queue framework,
with built-in AsyncAPI 3.0 schema generation.
</p>

<p align="center">
<a href="https://pypi.org/project/repid/" target="_blank">
    <img src="https://img.shields.io/pypi/v/repid.svg" alt="PyPI version">
</a>
<a href="https://github.com/aleksul/repid/actions" target="_blank">
    <img src="https://repid.aleksul.space/badges/coverage.svg" alt="Coverage">
</a>
<a href="https://pypi.python.org/pypi/repid/" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/repid.svg" alt="PyPI pyversions">
</a>
</p>

## Example

Here is how the easiest example of producer-consumer application can look like.

```python
import asyncio
from repid import Repid, Router, InMemoryServer

# 1. Initialize the application and register a server
app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

# 2. Create a router and an actor
router = Router()

@router.actor(channel="tasks")
async def my_awesome_actor(user_id: int) -> None:
    print(f"Processing for {user_id}")
    await asyncio.sleep(1.0)

app.include_router(router)

async def main() -> None:
    # 3. Open connection to interact with the queue
    async with app.servers.default.connection():
        # Producer: Send a message
        await app.send_message_json(
            channel="tasks",
            payload={"user_id": 123},
            headers={"topic": "my_awesome_actor"}
        )

        # Consumer: Run the worker loop (in a real app, this would be a separate process)
        await app.run_worker(messages_limit=1)

if __name__ == "__main__":
    asyncio.run(main())
```

## Install

**Repid** supports Python 3.10 and later. Install it with `pip`:

```bash
pip install repid
```

Or with `uv`:

```bash
uv add repid
```

Install extras for the integrations you use:

- `pydantic` - Pydantic deserialization support
- `amqp` - AMQP 1.0 broker support
- `nats` - NATS broker support
- `redis` - Redis broker support
- `pubsub` - GCP Pub/Sub broker support
- `kafka` - Apache Kafka broker support
- `sqs` - Amazon SQS broker support

```bash
pip install "repid[pydantic,amqp,nats,redis,pubsub,kafka,sqs]"
```

## Why Repid?

**Repid** gives async Python applications a clean task queue API with type hints, schema generation,
and broker flexibility built in.

- **AsyncAPI native**: Repid inspects your actors and generates AsyncAPI 3.0 schemas with interactive
  web documentation, so your task contracts stay close to the code that handles them.
- **Typed message handling**: With the `pydantic`, Repid can deserialize, validate, and coerce
  message payloads and headers before an actor runs.
- **Async-first performance**: Repid keeps the worker path small and async-native, avoiding sync
  compatibility layers around broker I/O and actor execution.
- **Dependency injection**: Actors can receive database connections, settings, clients, or other shared
  objects directly through their function signatures, keeping task code easier to test and reuse.
- **Broker flexibility**: Use Repid as a producer, a consumer, or both. Broker implementations sit
  behind the same application API, so business logic stays separate from transport code.

## LLMs

Repid documentation is available in text formats optimized for Large Language Models (LLMs)
and AI coding assistants:

- [llms.txt](llms.txt) - A concise overview and guide.
- [llms-full.txt](llms-full.txt) - The complete documentation in a single file.

## Inspiration

**Repid** is inspired by [`FastAPI`](https://github.com/tiangolo/fastapi),
[`dramatiq`](https://github.com/Bogdanp/dramatiq) and [`arq`](https://github.com/samuelcolvin/arq).

## License

**Repid** is distributed under the terms of the MIT license.
Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license.
It is originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
