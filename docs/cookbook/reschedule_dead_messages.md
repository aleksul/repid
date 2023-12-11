# Reschedule dead messages

Let's say for some reason you have messages in dead queue, which you want to reschedule
(i.e. try again). You can do it as follows:

```python hl_lines="13-14"
import asyncio

from repid import Repid, Queue, MessageCategory


app = Repid(my_connection)  # (1)


async def main() -> None:
    async with app.magic(auto_disconnect=True):
        q = Queue(name="my_queue")

        async for msg in q.get_messages(category=MessageCategory.DEAD):  # (2)
            await msg.reschedule()


if __name__ == "__main__":
    asyncio.run(main())
```

1. Connection setup is omitted
2. Will run indefinetly, you can exit with ++ctrl+c++

---

Alternativly, you can use `anext` and exit when you don't receive message for some time.

=== "Python 3.10+"

    ```python hl_lines="15 19"
    import asyncio

    from repid import Repid, Queue, MessageCategory

    EXIT_AFTER = 10  # seconds


    app = Repid(my_connection)  # (1)


    async def main() -> None:
        async with app.magic(auto_disconnect=True):
            q = Queue(name="my_queue")

            iterator = q.get_messages(category=MessageCategory.DEAD)

            while True:
                try:
                    msg = await asyncio.wait_for(anext(iterator), timeout=EXIT_AFTER)  # (2)
                except asyncio.TimeoutError:
                    break


    if __name__ == "__main__":
        asyncio.run(main())
    ```

    1. Connection setup is omitted
    2. Will raise an exception after specified amount of seconds, thus exiting the loop

=== "Python 3.8+"

    ```python hl_lines="15 19"
    import asyncio

    from repid import Repid, Queue, MessageCategory

    EXIT_AFTER = 10  # seconds


    app = Repid(my_connection)  # (1)


    async def main() -> None:
        async with app.magic(auto_disconnect=True):
            q = Queue(name="my_queue")

            iterator = q.get_messages(category=MessageCategory.DEAD)

            while True:
                try:
                    msg = await asyncio.wait_for(iterator.__anext__(), timeout=EXIT_AFTER)  # (2)
                except asyncio.TimeoutError:
                    break


    if __name__ == "__main__":
        asyncio.run(main())
    ```

    1. Connection setup is omitted
    2. Will raise an exception after specified amount of seconds, thus exiting the loop
