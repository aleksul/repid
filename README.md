<!-- markdownlint-disable MD033 -->

# repid

<a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
  <img alt="Repid's logo" width="350" align="right" src="https://raw.githubusercontent.com/gist/aleksul/fedbe168f1fc59c5aac3ddd17ecff30a/raw/b9467303f55517d99633d6551de223cd6534b149/repid_logo_borders.svg">
</a>

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![Coverage](https://repid.aleksul.space/badges/coverage.svg)](https://github.com/aleksul/repid/actions)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Read documentation](https://img.shields.io/badge/read-documentation-informational.svg)](https://repid.aleksul.space)

<br>

**Repid** is a simple, fast, and extensible async task queue framework,
with built-in [AsyncAPI 3.0](https://www.asyncapi.com/) schema generation.

<br>

```bash
pip install repid
```

## Features

- **AsyncAPI 3.0 out of the box** - your schema is generated automatically as you build.
  No separate spec files to maintain, no drift between code and docs.

- **Broker flexibility** - works with RabbitMQ, Redis, and Google Cloud Pub/Sub.
  Switch or run multiple brokers side by side without changing your actor code.

- **FastAPI-style ergonomics** - define actors as plain async functions with dependency
  injection and Pydantic argument validation. If you know FastAPI, you already know Repid.

- **Built for testability** - a drop-in `TestClient` lets you test actors in-memory
  without a running broker, making unit tests fast and dependency-free.

- **LLM-friendly documentation** - grab `llms.txt` or `llms-full.txt` from the docs site
  to feed into your favorite AI coding assistant.

## Quickstart

Here is how the simplest producer-consumer application looks, using AMQP server.

Producer:

```python
import asyncio

from repid import AmqpServer, Repid

app = Repid(title="My App", version="1.0.0")
app.servers.register_server(
    "default",
    AmqpServer("amqp://user:password@localhost:5672"),
    is_default=True,
)


async def main() -> None:
    async with app.servers.default.connection():
        await app.send_message(
            channel="default",
            payload=b"",
            headers={"topic": "awesome_job"},
        )


asyncio.run(main())
```

Consumer:

```python
import asyncio

from repid import AmqpServer, Repid, Router

app = Repid(title="My App", version="1.0.0")
app.servers.register_server(
    "default",
    AmqpServer("amqp://user:password@localhost:5672"),
    is_default=True,
)

router = Router()


@router.actor
async def awesome_job() -> None:
    print("Hello async jobs!")
    await asyncio.sleep(1.0)


app.include_router(router)


async def main() -> None:
    async with app.servers.default.connection():
        await app.run_worker()


asyncio.run(main())
```

Check out [user guide] to learn more!

## License

**Repid** is distributed under the terms of the MIT license. Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license.
It is originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[user guide]: https://repid.aleksul.space
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
