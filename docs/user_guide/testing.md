# Testing

Let's focus on testing our code with Repid.

## Preparation

Repid provides [pytest](https://pytest.org) plugin out-of-the box, but to use it
you will have to make sure you have pytest and [pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio)
plugin installed.

Optionally, you can specify repid to install with test dependencies:

```bash
pip install repid[test]
```

In the following examples will assume you have created an application with the following structure:

```bash
.
├── myapp
│   └── app.py
└── tests
    └── test_app.py
```

We will use a simple actor from one of the previous examples:

```python title="app.py"
from repid import Router

myrouter = Router()


@router.actor
async def actor_with_args(user_id: int, user_name: str, user_messages: list[str]) -> list[str]:
    user_message = f"Hi {user_name}! Your id is: {user_id}."
    user_messages.append(user_message)
    return user_messages
```

## Writing tests

We can use the fact that Repid doesn't anyhow modify actor function to our advantage and
write a very simple unit test.

```python title="test_app.py"
from myapp.app import actor_with_args


async def test_actor_with_args() -> None:
    expected = ["Hi Alex! Your id is: 123."]
    actual = actor_with_args(user_id=123, user_name="Alex", user_messages=[])
    assert actual == expected
```

However, we are not able to enqueue a `Job` to call our actor.
That's where Repid's pytest plugin comes to the rescue.

```python title="test_app.py"
import pytest
import repid
from myapp.app import actor_with_args, myrouter


@pytest.mark.repid  # (1)
async def test_actor_by_enqueueing_a_job() -> None:
    await repid.Queue().declare()

    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))
    await j.enqueue()  # (2)

    assert (await j.result) is None  # (3)

    await repid.Worker(routers=[myrouter], messages_limit=1).run()  # (4)

    assert (await j.result).data == ["Hi Alex! Your id is: 123."]  # (5)
```

1. Mark test function with Repid marker to activate some of the plugin features.
2. Enqueue a Job. The plugin takes care of creating an in-memory broker
    and passing the connection automatically.
3. The Job is enqueued but not yet processed, therefore the result is None.
4. For now, we manually create a Worker to process our Job.
    We will cover how to do it automatically in the next steps.
5. After the Job was processed, we should be able to retrieve the result.

If we pass our Router to the plugin's marker, the plugin will take care of the Job's processing.

```python hl_lines="6" title="test_app.py"
import pytest
import repid
from myapp.app import actor_with_args, myrouter


@pytest.mark.repid.with_args(routers=[myrouter])  # (1)
async def test_actor_by_enqueueing_a_job() -> None:
    await repid.Queue().declare()

    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))
    await j.enqueue()  # (2)

    assert (await j.result).data == ["Hi Alex! Your id is: 123."]
```

1. Using `.with_args(routers=[])` you are able to pass a list of routers to the plugin.
    Any Actor, included in those routers, will be automatically processed.
2. Processing of the enqueued message will happen just after it was enqueued,
    but before `.enqueue()` will return to the test function.

The plugin can also declare all the queues, which your passed routers are aware about.

```python hl_lines="6" title="test_app.py"
import pytest
import repid
from myapp.app import actor_with_args, myrouter


@pytest.mark.repid.with_args(routers=[myrouter], declare_all_known_queues=True)
async def test_actor_by_enqueueing_a_job() -> None:
    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))
    await j.enqueue()  # (1)

    assert (await j.result).data == ["Hi Alex! Your id is: 123."]
```

1. No need to declare a queue, plugin will take care of it.

## Using fixtures

??? info "What is a pytest fixture?"
    Pytest fixture is like a function, result of which can be injected in your tests.

    If you want to learn more about pytest fixtures, you can check out documentation
    [here](https://docs.pytest.org/en/latest/explanation/fixtures.html).

Repid provides a couple of pytest fixtures for you to use in your tests.

### Mocked actor fixture

When you pass a Router to Repid's testing plugin, it wraps all Actor calls in `MagicMock`.
To access the mock, use `repid_get_mocked_actor` fixture. You can then use the mock to assert calls,
specify side effects, etc.

```python title="test_app.py"
import pytest
import repid
from unittest.mock import MagicMock
from myapp.app import actor_with_args, myrouter


@pytest.mark.repid.with_args(routers=[r], declare_all_known_queues=True)
async def test_actor_with_mock(repid_get_mocked_actor: repid.GetMockedActorT) -> None:
    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))

    my_mock: MagicMock = repid_get_mocked_actor("actor_with_args")

    my_mock.assert_not_called()

    await j.enqueue()

    my_mock.assert_called_once_with(user_id=123, user_name="Alex", user_messages=[])
```

### Event log fixture

When testing some specific behavior of the library, you may want to ensure that all of the necessary
broker methods were called correctly. To do so, utilize `repid_get_event_log` fixture.

```python title="test_app.py"
import pytest
import repid
from myapp.app import actor_with_args, myrouter


@pytest.mark.repid.with_args(routers=[myrouter])
async def test_actor_with_event_log(repid_get_event_log: repid.GetEventLogT) -> None:
    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))
    await j.queue.declare()
    await j.enqueue()

    eventlog: list[repid.EventLog] = repid_get_event_log()  # (1)
    assert len(eventlog) == 7
    assert eventlog[0].function == "queue_declare"  # (2)
    assert eventlog[1].function == "store_bucket"
    assert eventlog[2].function == "enqueue"
    assert eventlog[3].function == "consume"
    assert eventlog[4].function == "get_bucket"
    assert eventlog[5].function == "ack"
    assert eventlog[6].function == "store_bucket"
```

1. Event log always comes sorted by the time of the execution.
2. See `repid.EventLog` for other possible arguments.

### In-memory queue fixture

By default you are using in-memory broker during your tests. If you want to get low-level access
to the underlying queue implementation you can use `repid_get_in_memory_queue` fixture.

```python title="test_app.py"
import pytest
import repid
from myapp.app import actor_with_args


@pytest.mark.repid  # (1)
async def test_get_queue(repid_get_in_memory_queue: repid.GetInMemoryQueueT) -> None:
    j = repid.Job("actor_with_args", args=dict(user_id=123, user_name="Alex", user_messages=[]))

    in_memory_queue = repid_get_in_memory_queue(j.queue)  # (2)
    assert in_memory_queue is None

    for _ in range(10):
        await j.enqueue()

    in_memory_queue = repid_get_in_memory_queue(j.queue)
    assert in_memory_queue.simple.qsize() == 10
```

1. We are intentionally not specifying our Router here, as otherwise all enqueued messages would've
    been processed and therefore retrieved from the queue.
2. You can also use string queue representation - `repid_get_in_memory_queue("default")`.

## Bigger test suites

You can mark whole module by specifying Repid's marker in [pytestmark](https://docs.pytest.org/en/latest/example/markers.html#marking-whole-classes-or-modules).

```python
import pytest
from myapp.app import myrouter

pytestmark = pytest.mark.repid.with_args(routers=[myrouter])
```

If for any reason you would like to disable automatic in-memory connection in a test -
set `autoconnection` to False:

```python
import pytest
from myapp.app import myrouter

pytestmark = pytest.mark.repid.with_args(routers=[myrouter])


@pytest.mark.repid.with_args(autoconnection=False)  # (1)
async def test_without_repid_connection() -> None:
    ...
```

1. The nearest marker to the test function is taken into account,
    when considering to disable autoconnection.
