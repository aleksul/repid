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

**Repid** supports Python versions 3.10 and up and is installable via `pip`.

```bash
pip install repid
```

There are also a couple of additional dependencies you may want to install,
depending on your use case:

```bash
pip install repid[amqp, redis, pubsub, pydantic]
```

## Why Repid?

**Repid** brings modern Python developer experience (DX) to background tasks, moving away from
legacy patterns and embracing strong typing, clear architecture, and flexibility.

- 📖 **AsyncAPI Native**: Stop maintaining outdated architecture diagrams. Repid automatically
  inspects your actors and generates AsyncAPI 3.0 schemas and interactive web documentation.
- 🛡️ **Modern Typing & Validation**: First-class Pydantic support. Your message payloads and headers
  are strictly validated and type-coerced before your actors even run.
- 💉 **Dependency Injection**: Write clean, testable code. Inject database connections, settings, or
  shared logic directly into your actor signatures.
- 🔌 **Highly Unopinionated**: Repid doesn't force a strict ecosystem. You can use it as just a
  producer, just a consumer, or easily swap out underlying message brokers without rewriting your
  business logic.

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
