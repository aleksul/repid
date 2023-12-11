# Message & Eager response

## Message inside of an Actor

There might be a situation where you would like to know the exact details of your incoming message.

You can get access to those details by specifying an argument, typed with `MessageDependency`.

```python hl_lines="5"
from repid import MessageDependency


@router.actor
async def my_actor(msg: MessageDependency) -> None:  # (1)
    ...
```

1. Message will be automatically injected into your actor

## Eager response

When you received a message inside of your actor, you have ability to trigger eager response, e.g.

```python hl_lines="3"
@router.actor
async def my_actor(msg: MessageDependency) -> None:
    await msg.ack()
    print("Hello?")  # (1)
```

1. Will not execute, as you have ack-ed the message, thus exiting actor's context.

However, `ack` is not the only available eager response, so here is the full list:

1. `ack` - Acknowledge
2. `nack` - Not acknowledge (i.e. put message in dead-letter queue)
3. `reject` - Reject receival (i.e. return the message to the initial queue)
4. `retry` - Retry execution (increments retry count, can raise `ValueError` if amount
of retries was exceeded)
5. `force_retry` - Retry execution even if amount of retries was exceeded
6. `reschedule` - Re-schedule the message. Can be useful if payload or parameters of the message
are changed.

### Setting result for eager response

If you would like to return a result of actor's execution, you can do one of the following:

#### Set a result

```python hl_lines="3"
@router.actor
async def my_actor(msg: MessageDependency) -> int:
    msg.set_result(123)
    await msg.ack()
```

#### Set an exception

```python hl_lines="3"
@router.actor
async def my_actor(msg: MessageDependency) -> int:
    msg.set_exception(Exception("Terrible error happened during execution!"))
    await msg.ack()
```

`set_result` or `set_exception` will be executed after an eager response. Only one can be used at a
time, and if set multiple times - only the latest will be executed.

### Adding callback to be executed after eager response

As eager response exits actor's scope, you won't be able to execute anything after it. To counteract
this, you can set any callable (both sync and async) as a callback using `add_callback` method:

```python hl_lines="11 13"
def my_callback() -> None:
    # do smth


async def my_async_callback() -> None:
    # do smth


@router.actor
async def my_actor(msg: MessageDependency) -> int:
    msg.add_callback(my_callback)
    msg.set_result(123)
    msg.add_callback(my_async_callback)
    await msg.ack()
```

Multiple callbacks can be set, and if so - they will be executed in the order of submission
(including `set_result` and `set_exception`).

Callbacks can't have any non-default arguments.
