from __future__ import annotations

from typing import Annotated, Any

import pytest
from annotated_types import Gt
from pydantic import ValidationError

from repid import Header, Message, Repid, Router
from repid.dependencies import Depends
from repid.test_client import TestClient

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_simple_message_dependency() -> None:
    app = Repid()
    router = Router()

    received_message_id = None

    @router.actor
    async def myactor(m: Message) -> None:
        nonlocal received_message_id
        received_message_id = m.message_id

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 1
        msg = client._sent_messages[0]
        assert received_message_id == msg.message_id


async def test_message_dependency_ack() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.ack()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 1
        msg = client._sent_messages[0]
        assert msg.acked


async def test_message_dependency_nack() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.nack()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 1
        msg = client._sent_messages[0]
        assert msg.nacked


async def test_message_dependency_reject() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.reject()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 1
        msg = client._sent_messages[0]
        assert msg.rejected


async def test_message_dependency_send_message() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.send_message_json(
            channel="other_channel",
            payload={"foo": "bar"},
            headers={"topic": "other_actor"},
        )

    received_other = False

    @router.actor(channel="other_channel")
    async def other_actor(m: Message) -> None:
        nonlocal received_other
        received_other = True
        assert m.payload == b'{"foo":"bar"}'

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 2

        # Process the second message manually
        while not client._message_queue.empty():
            _, msg = await client._message_queue.get()
            await client._process_message(msg)

        assert received_other


async def test_message_dependency_reply() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.reply_json(payload={"response": "ok"})

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        assert len(client._sent_messages) == 2
        msg = client._sent_messages[0]
        assert len(msg._reply_messages) == 1
        channel, reply_msg = msg._reply_messages[0]
        assert channel == "default"
        assert reply_msg.payload == b'{"response":"ok"}'


def _mydependency() -> str:
    return "aaa"


async def test_depends() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(arg1: str, d: Annotated[str, Depends(_mydependency)]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "hello"},
            headers={"topic": "myactor"},
        )

    assert received == "aaa"


async def _sub_dependency() -> str:
    return "sub"


async def _dependency(sub: Annotated[str, Depends(_sub_dependency)]) -> str:
    return f"dep_{sub}"


async def test_sub_depends() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(d: Annotated[str, Depends(_dependency)]) -> None:
        nonlocal received
        received = d

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "myactor"},
        )

    assert received == "dep_sub"


def test_non_default_arg() -> None:
    def mysubdependency(m: str = "default") -> str:
        return f"{m}"

    # Args with defaults are allowed
    dep = Depends(mysubdependency)
    assert dep is not None


async def test_multiple_depends() -> None:
    app = Repid()
    router = Router()

    received = None
    received2 = None

    async def mydependency() -> str:
        return "aaa"

    def myseconddependency() -> int:
        return 123

    @router.actor
    async def myactor(
        arg1: str,  # noqa: ARG001
        d: Annotated[str, Depends(mydependency)],
        d2: Annotated[int, Depends(myseconddependency)],
    ) -> None:
        nonlocal received, received2
        received = d
        received2 = d2

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "hello"},
            headers={"topic": "myactor"},
        )

    assert received == "aaa"
    assert received2 == 123


async def test_dependency_override() -> None:
    app = Repid()
    router = Router()

    received = None

    async def mydependency() -> str:
        return "aaa"

    def another_dependency() -> str:
        return "bbb"

    dep = Depends(mydependency)

    @router.actor
    async def myactor(arg1: str, d: Annotated[str, dep]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    dep.override(another_dependency)

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg1": "hello"},
            headers={"topic": "myactor"},
        )

    assert received == "bbb"


async def test_dependency_with_default_value() -> None:
    app = Repid()
    router = Router()

    received = None

    def mydependency(value: int = 42) -> int:
        return value * 2

    @router.actor
    async def myactor(d: Annotated[int, Depends(mydependency)]) -> None:
        nonlocal received
        received = d

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

    assert received == 84


async def test_async_dependency() -> None:
    app = Repid()
    router = Router()

    received = None

    async def mydependency() -> str:
        return "async_value"

    @router.actor
    async def myactor(d: Annotated[str, Depends(mydependency)]) -> None:
        nonlocal received
        received = d

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

    assert received == "async_value"


async def test_dependency_injection_complex() -> None:
    router = Router()

    # Capture values to assert later
    captured_values = {}

    async def sub_dependency_function(
        other: int,
        ommitted_param: str = "default",
        third_header: Annotated[int, Header()] = 3,
    ) -> int:
        captured_values["sub_dependency"] = {
            "other": other,
            "ommitted_param": ommitted_param,
            "third_header": third_header,
        }
        return 42

    async def dependency_function(
        what: str,
        sub_dep: Annotated[int, Depends(sub_dependency_function)],
        other_header: Annotated[int, Gt(0), Header(name="another_one")] = 1,
    ) -> str:
        captured_values["dependency"] = {
            "what": what,
            "sub_dep": sub_dep,
            "other_header": other_header,
        }
        return "Dependency Resolved"

    @router.actor
    async def useless(
        some_header: Annotated[int, Header(name="other_name")],
        dependency: Annotated[str, Depends(dependency_function)],
    ) -> None:
        captured_values["actor"] = {
            "some_header": some_header,
            "dependency": dependency,
        }

    app = Repid()
    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"what": "Hello", "other": "123"},
            headers={"topic": "useless", "other_name": "1234567", "another_one": "42"},
        )

    # Assertions
    assert captured_values["sub_dependency"]["other"] == 123
    assert captured_values["sub_dependency"]["ommitted_param"] == "default"
    assert captured_values["sub_dependency"]["third_header"] == 3

    assert captured_values["dependency"]["what"] == "Hello"
    assert captured_values["dependency"]["sub_dep"] == 42
    assert captured_values["dependency"]["other_header"] == 42

    assert captured_values["actor"]["some_header"] == 1234567
    assert captured_values["actor"]["dependency"] == "Dependency Resolved"


async def test_header_dependency() -> None:
    app = Repid()
    router = Router()

    received_headers: dict[str, Any] = {}

    @router.actor
    async def myactor(
        h1: Annotated[str, Header()],
        h2: Annotated[str, Header(name="custom-header")],
        h3: Annotated[int, Header()] = 123,
        h4: Annotated[str, Header()] = "default",
    ) -> None:
        received_headers["h1"] = h1
        received_headers["h2"] = h2
        received_headers["h3"] = h3
        received_headers["h4"] = h4

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "myactor", "h1": "value1", "custom-header": "value2", "h3": "456"},
        )

    assert received_headers["h1"] == "value1"
    assert received_headers["h2"] == "value2"
    assert received_headers["h3"] == 456
    assert received_headers["h4"] == "default"


async def test_header_dependency_validation_error() -> None:
    app = Repid()
    router = Router()

    received = False

    @router.actor
    async def myactor(h: Annotated[int, Header()]) -> None:  # noqa: ARG001
        nonlocal received
        received = True

    app.include_router(router)

    async with TestClient(app) as client:
        with pytest.raises(ValidationError):
            await client.send_message_json(
                channel="default",
                payload={},
                headers={"topic": "myactor", "h": "not-an-int"},
            )

    assert not received
