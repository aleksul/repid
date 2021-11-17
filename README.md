# repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![Tests](https://github.com/aleksul/repid/actions/workflows/tests.yaml/badge.svg)](https://github.com/aleksul/repid/actions/workflows/tests.yaml)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The `repid` package is an async Redis queue for Python, built around `aioredis`.

```bash
pip install repid
```

It can be easily used near to existing `aioredis` instances. Integration with other packages (such as `fastapi`) is quite simple too!

## Usage

On producer side:

```python
import repid
import asyncio
from aioredis import Redis

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
from aioredis import Redis

myredis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
myworker = repid.Worker(myredis)

@myworker.actor()
async def my_first_job():
    return "Hello Repid!"

asyncio.run(myworker.run_forever())
```

It's important to specify `decode_responses=True` because `repid` relaies on parsed data.
