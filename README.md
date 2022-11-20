<a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
  <img alt="Repid's logo" width="350" align="right" src="https://gist.github.com/aleksul/fedbe168f1fc59c5aac3ddd17ecff30a/raw/b9467303f55517d99633d6551de223cd6534b149/repid_logo_borders.svg">
</a>

# repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<br>

**Repid** framework: simple to use, fast to run and extensible to adopt job scheduler.

<br>

```bash
pip install repid
```

## Quickstart

Here is how the easiest example of producer-consumer application can look like.

Producer:

```python
import asyncio

from repid import Connection, Job, RabbitMessageBroker, Repid

app = Repid(Connection(RabbitMessageBroker("amqp://user:password@localhost:5672")))


async def main() -> None:
    async with app.magic():
        await Job(name="awesome_job").enqueue()


asyncio.run(main())
```

Consumer:

```python
import asyncio

from repid import Connection, RabbitMessageBroker, Repid, Router, Worker

app = Repid(Connection(RabbitMessageBroker("amqp://user:password@localhost:5672")))
router = Router()


@router.actor
async def awesome_job() -> None:
    print("Hello async jobs!")
    await asyncio.sleep(1.0)


async def main() -> None:
    async with app.magic():
        await Worker(routers=[router]).run()


asyncio.run(main())
```

Check out [user guide] to learn more!

## License

**Repid** is distributed under the terms of the MIT license. Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license. It is originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[user guide]: https://repid.aleksul.space
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
