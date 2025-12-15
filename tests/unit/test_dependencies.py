from __future__ import annotations

from typing import Annotated, Any

import pytest
from annotated_types import Gt
from pydantic import ValidationError

from repid import Header, Message, Repid, Router
from repid.converter import BasicConverter, DefaultConverter
from repid.data import MessageData
from repid.dependencies import Depends, MessageDependency
from repid.dependencies._utils import get_dependency
from repid.test_client import TestClient


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


async def test_depends() -> None:
    app = Repid()
    router = Router()

    received = None

    def _mydependency() -> str:
        return "aaa"

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


async def test_sub_depends() -> None:
    app = Repid()
    router = Router()

    received = None

    async def _sub_dependency() -> str:
        return "sub"

    async def _dependency(sub: Annotated[str, Depends(_sub_dependency)]) -> str:
        return f"dep_{sub}"

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


def test_positional_only_arg_raises() -> None:
    def mysubdependency(m: str, /) -> str:
        return f"{m}"

    with pytest.raises(
        ValueError,
        match="Positional-only arguments in dependencies are not supported",
    ):
        Depends(mysubdependency)


async def test_depends_with_default_arg_not_in_payload() -> None:
    app = Repid()
    router = Router()

    received = None

    def mydep(x: str, y: str = "default_value") -> str:
        return f"{x}_{y}"

    @router.actor(converter=BasicConverter)  # use basic converter to avoid pydantic defaults
    async def myactor(d: Annotated[str, Depends(mydep)]) -> None:
        nonlocal received
        received = d

    app.include_router(router)

    async with TestClient(app) as client:
        # Only provide x, not y
        await client.send_message_json(
            channel="default",
            payload={"x": "hello"},
            headers={"topic": "myactor"},
        )

    # Should use the default value for y
    assert received == "hello_default_value"


async def test_depends_with_basic_converter_missing_arg() -> None:
    app = Repid()
    router = Router()

    def mydep(x: str) -> str:
        return x

    @router.actor(converter=BasicConverter)  # use basic converter to avoid pydantic validation
    async def myactor(d: Annotated[str, Depends(mydep)]) -> None:
        pass

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        # Don't provide x
        await client.send_message_json(
            channel="default",
            payload={},
            headers={"topic": "myactor"},
        )

        # Should have raised an error
        processed = client.get_processed_messages()
        assert len(processed) == 1
        assert processed[0].exception is not None
        assert "Missing required argument 'x'" in str(processed[0].exception)


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


async def test_annotated_without_dependency() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(arg: Annotated[int, Gt(3)]) -> None:
        nonlocal received
        received = arg

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"arg": "15"},
            headers={"topic": "myactor"},
        )

    assert received == 15


@pytest.mark.parametrize("dependency_cls", [Header, MessageDependency])
async def test_class_instead_of_instance_dependency_raises_warning(
    dependency_cls: type[Header | MessageDependency],
) -> None:
    router = Router()

    with pytest.warns(
        UserWarning,
        match="Using Header or Message classes directly as dependency has no effect.",
    ):

        @router.actor
        async def myactor(msg: Annotated[int, dependency_cls]) -> None:
            pass


async def test_message_properties() -> None:
    app = Repid()
    router = Router()

    captured = {}

    @router.actor
    async def myactor(m: Message) -> None:
        nonlocal captured
        captured = {
            "payload": m.payload,
            "headers": m.headers,
            "content_type": m.content_type,
            "channel": m.channel,
            "is_acted_on": m.is_acted_on,
        }

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(
            channel="default",
            payload={"key": "value"},
            headers={"topic": "myactor", "custom": "header"},
        )

    assert captured == {
        "payload": b'{"key":"value"}',
        "headers": {"topic": "myactor", "custom": "header"},
        "content_type": "application/json",
        "channel": "default",
        "is_acted_on": False,
    }


async def test_message_send_message_raw() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.send_message(
            channel="other",
            payload=b"raw bytes",
            headers={"topic": "other_actor"},
            content_type="application/octet-stream",
        )

    @router.actor(channel="other")
    async def other_actor(m: Message) -> None:
        pass

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        # Should have 2 messages: original + one sent by actor
        assert len(client._sent_messages) == 2


async def test_message_reply_raw() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(m: Message) -> None:
        await m.reply(
            payload=b"raw response",
            content_type="application/octet-stream",
        )

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message_json(channel="default", payload={}, headers={"topic": "myactor"})

        msg = client._sent_messages[0]
        assert len(msg._reply_messages) == 1
        _, reply = msg._reply_messages[0]
        assert reply.payload == b"raw response"


async def test_depends_missing_required_argument() -> None:
    def _dep_with_required_arg(required_arg: str) -> str:
        return required_arg

    async def _fn_with_required_dep_arg(d: Annotated[str, Depends(_dep_with_required_arg)]) -> None:
        pass

    conv = DefaultConverter(_fn_with_required_dep_arg, correlation_id=None, fn_locals=locals())

    with pytest.raises(ValueError, match="Field required"):
        await conv.convert_inputs(
            message=MessageData(  # type: ignore[arg-type]
                payload=b"{}",  # empty payload, missing required_arg
                headers=None,
                content_type="application/json",
            ),
            actor=None,  # type: ignore[arg-type]
            server=None,  # type: ignore[arg-type]
            default_serializer=None,  # type: ignore[arg-type]
        )


async def test_depends_uses_default_for_missing_argument() -> None:
    def _dep_with_default_arg(arg_with_default: str = "default_value") -> str:
        return arg_with_default

    async def _fn_with_default_dep_arg(d: Annotated[str, Depends(_dep_with_default_arg)]) -> None:
        pass

    conv = DefaultConverter(_fn_with_default_dep_arg, correlation_id=None, fn_locals=locals())

    # Send message without arg_with_default - should use default
    args, kwargs = await conv.convert_inputs(
        message=MessageData(  # type: ignore[arg-type]
            payload=b"{}",
            headers=None,
            content_type="application/json",
        ),
        actor=None,  # type: ignore[arg-type]
        server=None,  # type: ignore[arg-type]
        default_serializer=None,  # type: ignore[arg-type]
    )
    assert args == []
    assert kwargs == {"d": "default_value"}


def test_get_dependency_skips_class_metadata() -> None:
    # Using Header class instead of Header() instance should return None
    result = get_dependency(Annotated[str, Header])
    assert result is None

    # Using Header() instance should return the instance
    header_instance = Header()
    result = get_dependency(Annotated[str, header_instance])
    assert result is header_instance
