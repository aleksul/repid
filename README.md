<a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
  <img alt="Repid's logo" width="350" align="right" src="https://gist.github.com/aleksul/2e4686cf9a4f027909fe43dc33039f10/raw/56935b8183682d1e46d68af70fec52cf647ab756/repid_logo.svg">
</a>

# repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<br>

**Repid** is a job queueing library for Python with focus on simplicity.

<br>

```bash
pip install repid
```

## Quickstart

Here is how the easiest example of producer-consumer application can look like.

Producer:

```python
import asyncio
from repid import Repid, Connection, RabbitBroker, Job

myrepid = Repid(Connection(RabbitBroker("amqp://user:password@localhost:5672")))

async def main():
  async with myrepid.connect():
    await Job(name="awesome_job").enqueue()

asyncio.run(main())
```

Consumer:

```python
import asyncio
from repid import Repid, Router, Connection, RabbitBroker, Worker, Job

myrepid = Repid(Connection(RabbitBroker("amqp://user:password@localhost:5672")))

r = Router()

@r.actor
async def awesome_job() -> None:
  print("Hello async jobs!")
  await do_some_async_stuff()

async def main():
  async with myrepid.connect():
    w = Worker(routers=[r])
    await w.run()

asyncio.run(main())
```

Check out [user guide] to learn more!

## License

**Repid** is distributed under the terms of the MIT license. Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license. It was originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[user guide]: https://aleksul.github.io/repid
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
