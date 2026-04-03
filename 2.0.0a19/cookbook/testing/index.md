# Testing

Repid provides a built-in `TestClient` to make testing your actors and application logic simple and fast. You don't need any complex pytest plugins or real message brokers.

## Preparation

In the following examples, we will assume you have created an application with the following structure:

```
.
├── myapp
│   └── app.py
└── tests
    └── test_app.py
```

We will use a simple actor defined on our `Repid` application instance:

myapp/app.py

```
from repid import Repid, Router

app = Repid(title="My App")
router = Router()

@router.actor(channel="user_messages")
async def actor_with_args(user_id: int, user_name: str, user_messages: list[str]) -> list[str]:
    user_message = f"Hi {user_name}! Your id is: {user_id}."
    user_messages.append(user_message)
    return user_messages

app.include_router(router)
```

## Using the TestClient

The `TestClient` allows you to send messages directly to your application without needing a running server. It intercepts the messages and can either automatically process them, or let you process them manually one-by-one.

tests/test_app.py

```
import pytest
from repid import TestClient
from myapp.app import app

async def test_actor_processing() -> None:
    # 1. Initialize the TestClient using your app as the parameter
    async with TestClient(app) as client:
        # 2. Send a message to the channel
        await client.send_message_json(
            channel="user_messages",
            payload=dict(user_id=123, user_name="Alex", user_messages=[]),
            headers={"topic": "actor_with_args"}
        )

        # 3. By default (auto_process=True), the client instantly processes messages.
        # Let's verify the processed messages.
        processed = client.get_processed_messages()

        assert len(processed) == 1
        assert processed[0].success is True
        assert processed[0].result == ["Hi Alex! Your id is: 123."]
```

### Manual Message Processing

If you want to control exactly when messages are processed (e.g. to test queue buildup or chaining), you can set `auto_process=False` on the test client.

tests/test_app.py

```
import pytest
from repid import TestClient
from myapp.app import app

async def test_manual_processing() -> None:
    async with TestClient(app, auto_process=False) as client:
        await client.send_message_json(
            channel="user_messages",
            payload=dict(user_id=123, user_name="Alex", user_messages=[]),
            headers={"topic": "actor_with_args"}
        )

        # The message is in the queue, but hasn't been processed yet
        assert len(client.get_processed_messages()) == 0

        # Process the next message in the queue
        msg = await client.process_next()

        assert msg is not None
        assert msg.success is True
        assert msg.result == ["Hi Alex! Your id is: 123."]
```

### Inspecting Messages

The `TestMessage` object returned by the `TestClient` tracks the full lifecycle of the message. You can assert against various properties to make sure your actors behave correctly:

```
msg = await client.process_next()

assert msg.acked is True        # Was it acknowledged?
assert msg.nacked is False      # Was it NACKed?
assert msg.rejected is False    # Was it rejected?
assert msg.exception is None    # Did it raise an exception?
assert msg.result == ["..."]    # The return value of the actor
```

You can retrieve all sent or processed messages using:

- `client.get_sent_messages()`
- `client.get_processed_messages()`

Both functions optionally accept a `channel` or `operation_id` argument to filter the list of messages.

## Unit Testing Actors

Remember that Repid doesn't modify your actor functions in any way. You can always write simple, isolated unit tests by importing your function and calling it directly:

tests/test_app.py

```
from myapp.app import actor_with_args

async def test_actor_with_args() -> None:
    expected = ["Hi Alex! Your id is: 123."]
    actual = await actor_with_args(user_id=123, user_name="Alex", user_messages=[])
    assert actual == expected
```
