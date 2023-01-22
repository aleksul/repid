<!-- markdownlint-disable MD033 -->
# Repid

<p align="center">
  <a href="https://www.instagram.com/p/Cd-ob1NNZ84/">
    <img alt="Repid's logo" width=400 src="https://gist.github.com/aleksul/fedbe168f1fc59c5aac3ddd17ecff30a/raw/b9467303f55517d99633d6551de223cd6534b149/repid_logo_borders.svg">
  </a>
</p>

<p align="center">
<b>Repid</b> framework: simple to use, fast to run and extensible to adopt job scheduler.
</p>

<p align="center">
<a href="https://pypi.org/project/repid/" target="_blank">
    <img src="https://img.shields.io/pypi/v/repid.svg" alt="PyPI version">
</a>
<a href="https://codecov.io/gh/aleksul/repid" target="_blank">
    <img src="https://codecov.io/gh/aleksul/repid/branch/main/graph/badge.svg?token=IP3Z1VXB1G" alt="codecov">
</a>
<a href="https://pypi.python.org/pypi/repid/" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/repid.svg" alt="PyPI pyversions">
</a>
</p>

## Example

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

## Install

**Repid** supports Python versions 3.8 and up and is installable via `pip`.

```bash
pip install repid
```

There are also a couple of additional dependencies you may want to install,
depending on your use case, e.g.

```bash
pip install repid[amqp, redis, cron]
```

## Why repid?

### Asyncio

`Repid` is built around `asyncio`. It means it's pretty fast.
And you don't have to worry that it will slow down your other asyncio-driven code.

### Ease of integration

There is an abstraction layer on top of other queue solutions.
It means that even if `repid` doesn't provide some broker out of the box,
you will be able to write your own.

### Built with microservices in mind

Your producer and consumer can be running in different containers, `repid` will handle it just fine.

### Can be used with other languages

`Repid` uses json (de-)serialization by default, which makes integration with other languages
as easy as possible. You're also able to easily override default (de-)serialization
behavior thanks to PEP 544 Protocols.

### Integrated scheduling

`Repid` has its own scheduling mechanisms.
You can delay job execution until some date or even execute it every once in a while.
No need for extra dependencies!

## Inspiration

`Repid` is inspired by [`dramatiq`](https://github.com/Bogdanp/dramatiq) and [`arq`](https://github.com/samuelcolvin/arq).

## License

**Repid** is distributed under the terms of the MIT license. Please see [License.md] for more information.

**Repid's logo** is distributed under the terms of the [CC BY-NC 4.0] license.
It is originally created by [ari_the_crow_].

[License.md]: https://github.com/aleksul/repid/blob/master/LICENSE
[CC BY-NC 4.0]: https://creativecommons.org/licenses/by-nc/4.0/
[ari_the_crow_]: https://www.instagram.com/p/Cd-ob1NNZ84/
