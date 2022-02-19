# Repid

[![PyPI version](https://img.shields.io/pypi/v/repid.svg)](https://pypi.org/project/repid/)
[![codecov](https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G)](https://codecov.io/gh/aleksul/repid)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/repid.svg)](https://pypi.python.org/pypi/repid/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Repid** is a job queuing library for Python with focus on simplicity.

## Example

Here's what a smallest (but already working) peace of code can look like:

```python
import repid
import asyncio
from aioredis import Redis

# create redis instance as usual
myredis = Redis(host="localhost", port=6379, db=0, decode_responses=True)

# create repid worker using redis instance
myworker = repid.Worker(myredis)

# add a simple actor to the worker
@myworker.actor()
async def my_first_job():
    return "Hello Repid!"

# enqueue a job...
asyncio.run(myworker.enqueue_job("my_first_job"))

# ...and run the worker
asyncio.run(myworker.run_forever())
```

## Install

**Repid** supports Python versions 3.8 and up and installable via either `pip` or `poetry`.

```bash
pip install repid
```

```bash
poetry add repid
```

## Why repid?

### Asyncio

`Repid` is built around `asyncio`. It means it's pretty fast.
And you don't have to worry that it will slow down your other asyncio-driven code.

### Ease of integration

You are probably already used to `aioredis`. Why don't you utilize it?
Just pass its instance to `repid` and you are ready to go!

### Built with microservices in mind

Your producer and consumer can be running in different containers, `repid` will handle it just fine.

### Can be used with other languages

`Repid` doesn't use pickling, instead all data is parsed into json, which are documented [here]().
It means you can easily adopt it for other languages.

### Integrated scheduling

`Repid` has its own scheduling mechanisms.
You can delay job execution until some date or even execute it every once in a while.
No need for extra dependencies!

### Small

`Repid` tries to be as small as possible.
It makes it simpler to learn, simpler to test and simpler to use. So what are you waiting for?

## Inspiration

`Repid` is inspired by [`dramatiq`](https://github.com/Bogdanp/dramatiq) and [`arq`](https://github.com/samuelcolvin/arq).

## License

This project is licensed under the terms of the MIT license.
