<a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://gist.github.com/aleksul/47ef6993e13a23371762176e7e206b30/raw/f0f2265e4cb22dd2f2886027965d54379fc6a688/repid_dark_logo.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://gist.github.com/aleksul/47ef6993e13a23371762176e7e206b30/raw/f0f2265e4cb22dd2f2886027965d54379fc6a688/repid_light_logo.svg">
    <img alt="Repid's logo" width="350" align="right">
  </picture>
</a>

# repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<br>

**Repid** is a job queuing library for Python with focus on simplicity.

<br>

```bash
pip install repid
```

## Quickstart

Here is how the easiest example of producer-consumer application can look like.

Producer:

```python
import asyncio
from repid import Repid, Job

Repid("amqp://user:password@localhost:5672")

async def main():
  myjob = Job(name="awesome_job")
  await myjob.queue.declare()
  await myjob.enqueue()

asyncio.run(main())
```

Consumer:

```python
import asyncio
from repid import Repid, Worker, Job

Repid("amqp://user:password@localhost:5672")

myworker = Worker()

@myworker.actor()
async def awesome_job() -> None:
  print("Hello async jobs!")
  await do_some_async_stuff()

asyncio.run(myworker.run())
```

Check out [user guide] to learn more!

## License

**Repid** is distributed under the terms of the MIT license. Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license. It was originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[user guide]: https://aleksul.github.io/repid
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
