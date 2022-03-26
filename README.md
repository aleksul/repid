# repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Repid** is a job queuing library for Python with focus on simplicity.

```bash
pip install repid
```

## Quickstart

Make sure Redis is running, then start 2 processes/containers with code as below.

On producer side:

```python
import repid
import asyncio
from redis.asyncio import Redis

myredis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
myrepid = repid.Repid(myredis)

async def main():
    await myrepid.enqueue("my_first_job")

asyncio.run(main())
```

On consumer side:

```python
import repid
import asyncio
from redis.asyncio import Redis

myredis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
myworker = repid.Worker(myredis)

@myworker.actor()
async def my_first_job():
    return "Hello Repid!"

asyncio.run(myworker.run_forever())
```

Check out [user guide] to learn more!

## License

**Repid** is licensed under the MIT. Please see [License.md] for more information.

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[user guide]: http://aleksul.github.io/repid
