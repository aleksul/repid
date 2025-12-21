from typing import Annotated

import pytest

from repid import Header, Message, Repid, Router
from repid.test_client import TestClient


async def test_test_client_basic() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received
        received = arg1

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b'{"arg1": "hello"}',
            headers={"topic": "myactor"},
        )

    assert received == "hello"


async def test_test_client_with_router() -> None:
    router = Router()

    received = None

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received
        received = arg1

    app = Repid()
    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message(
            operation_id="myactor",
            payload=b'{"arg1": "world"}',
            headers={"topic": "myactor"},
        )

    assert received == "world"


async def test_test_client_json() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(key: str) -> None:
        nonlocal received
        received = key

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"key": "value"},
            headers={"topic": "myactor"},
        )

    assert received == "value"


async def test_test_client_with_headers() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(
        arg1: str,  # noqa: ARG001
        x_custom: Annotated[str, Header(name="X-Custom")],
    ) -> None:
        nonlocal received
        received = x_custom

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "hello"},
            headers={"topic": "myactor", "X-Custom": "header"},
        )

    assert received == "header"


async def test_test_client_unknown_operation() -> None:
    app = Repid()

    async with TestClient(app) as client:
        with pytest.raises(ValueError, match="Operation 'unknown_actor' not found"):
            await client.send_message_json(operation_id="unknown_actor", payload={})


async def test_test_client_multiple_messages() -> None:
    app = Repid()
    router = Router()

    received_messages = []

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received_messages
        received_messages.append(arg1)

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "first"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "second"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "third"},
            headers={"topic": "myactor"},
        )

    assert received_messages == ["first", "second", "third"]


async def test_test_client_process_next_and_clear() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(arg1: str) -> None:
        pass

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    client = TestClient(app, auto_process=False)

    async with client:
        # send two messages and ensure they are queued
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "first"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "second"},
            headers={"topic": "myactor"},
        )

        # process next should process first queued message
        processed = await client.process_next()
        assert processed is not None
        assert processed.result is None or processed.success is True

        # process all should process remaining
        processed_messages = await client.process_all()
        assert isinstance(processed_messages, list)

        # clear should remove any queued messages and state
        client.clear()
        assert client.get_sent_messages() == []
        assert client.get_processed_messages() == []


async def test_test_client_reply_queues_new_message_and_error_handling() -> None:
    app = Repid()
    router = Router()

    processed = False

    @router.actor
    async def dummy_actor(message: Message) -> None:
        await message.reply(payload=b"", headers={"topic": "reply_actor"})

    @router.actor
    async def reply_actor() -> None:
        nonlocal processed
        processed = True

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b"",
            headers={"topic": "dummy_actor"},
        )
        assert processed is False
        # processing of the original message has queued a reply message, process it now
        await client.process_next()
        assert processed is True


async def test_test_client_actor_exception() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def failing_actor() -> None:
        raise RuntimeError("Actor error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=False) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "failing_actor"},
        )

        processed = client.get_processed_messages()
        assert len(processed) == 1
        assert processed[0].exception is not None
        assert isinstance(processed[0].exception, RuntimeError)


async def test_test_client_actor_exception_with_raise() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def failing_actor() -> None:
        raise RuntimeError("Actor error")

    app.include_router(router)

    async with TestClient(app, raise_on_actor_error=True) as client:
        with pytest.raises(RuntimeError, match="Actor error"):
            await client.send_message(
                channel="default",
                payload=b"{}",
                headers={"topic": "failing_actor"},
            )

        assert len(client.get_processed_messages()) == 1


async def test_test_client_no_actor_found() -> None:
    app = Repid()

    async with TestClient(app, raise_on_actor_not_found=False) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
        )

        assert len(client.get_sent_messages()) == 1
        assert len(client.get_processed_messages()) == 0


async def test_test_client_no_actor_found_for_channel() -> None:
    app = Repid()
    router = Router()

    # Register an actor for a different channel
    @router.actor(channel="other_channel")
    async def other_actor() -> None:
        pass

    app.include_router(router)

    async with TestClient(app, raise_on_actor_not_found=False) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "nonexistent"},
        )

        assert len(client.get_sent_messages()) == 1
        assert len(client.get_processed_messages()) == 0


async def test_test_client_no_actor_for_channel_raises() -> None:
    app = Repid()
    router = Router()

    # Register an actor for a different channel
    @router.actor(channel="other_channel")
    async def other_actor() -> None:
        pass

    app.include_router(router)

    async with TestClient(app, auto_process=False, raise_on_actor_not_found=True) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "nonexistent"},
        )

        # Processing should raise because no actor for this channel
        with pytest.raises(ValueError, match="No actor found for channel 'default'"):
            await client.process_next()


async def test_test_client_message_ack() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="manual")
    async def manual_actor(message: Message) -> None:
        await message.ack()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "manual_actor"},
        )

        processed = client.get_processed_messages()
        assert len(processed) == 1
        assert processed[0].acked is True


async def test_test_client_message_nack() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="manual")
    async def nack_actor(message: Message) -> None:
        await message.nack()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "nack_actor"},
        )

        processed = client.get_processed_messages()
        assert len(processed) == 1
        assert processed[0].nacked is True


async def test_test_client_message_reject() -> None:
    app = Repid()
    router = Router()

    @router.actor(confirmation_mode="manual")
    async def reject_actor(message: Message) -> None:
        await message.reject()

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b"{}",
            headers={"topic": "reject_actor"},
        )

        processed = client.get_processed_messages()
        assert len(processed) == 1
        assert processed[0].rejected is True


async def test_test_client_message_properties() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def prop_actor() -> str:
        return "result"

    app.include_router(router)
    app.messages.register_operation(operation_id="prop_actor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="prop_actor",
            payload={},
            headers={"topic": "prop_actor", "custom": "value"},
        )

        processed = client.get_processed_messages()
        assert len(processed) == 1
        msg = processed[0]
        assert msg.channel == "default"
        assert msg.operation_id == "prop_actor"
        assert msg.headers == {"topic": "prop_actor", "custom": "value"}
        assert msg.content_type == "application/json"
        assert msg.message_id is not None
        assert msg.timestamp is not None
        assert msg.is_acted_on is True
        assert msg.success is True
        assert msg.result == "result"


async def test_test_client_get_filtered_messages() -> None:
    app = Repid()
    router = Router()

    @router.actor(channel="channel_a")
    async def actor_a() -> None:
        pass

    @router.actor(channel="channel_b")
    async def actor_b() -> None:
        pass

    app.include_router(router)
    app.messages.register_operation(operation_id="actor_a", channel="channel_a")
    app.messages.register_operation(operation_id="actor_b", channel="channel_b")

    async with TestClient(app) as client:
        await client.send_message(
            operation_id="actor_a",
            payload=b"{}",
            headers={"topic": "actor_a"},
        )
        await client.send_message(
            operation_id="actor_b",
            payload=b"{}",
            headers={"topic": "actor_b"},
        )

        all_sent = client.get_sent_messages()
        assert len(all_sent) == 2

        filtered_sent = client.get_sent_messages(operation_id="actor_a")
        assert len(filtered_sent) == 1

        filtered_sent_channel = client.get_sent_messages(channel="channel_a")
        assert len(filtered_sent_channel) == 1

        all_processed = client.get_processed_messages()
        assert len(all_processed) == 2

        filtered_processed = client.get_processed_messages(operation_id="actor_b")
        assert len(filtered_processed) == 1

        filtered_processed_channel = client.get_processed_messages(channel="channel_b")
        assert len(filtered_processed_channel) == 1


async def test_test_client_send_without_channel_or_operation_id_raises() -> None:
    app = Repid()

    async with TestClient(app) as client:
        with pytest.raises(
            ValueError,
            match="Either 'channel' or 'operation_id' must be specified",
        ):
            await client.send_message(  # type: ignore[call-overload]
                payload=b"{}",  # Neither channel nor operation_id
            )


async def test_test_client_send_with_both_channel_and_operation_id_raises() -> None:
    app = Repid()

    async with TestClient(app) as client:
        with pytest.raises(
            ValueError,
            match="Specify either 'channel' or 'operation_id', not both",
        ):
            await client.send_message(  # type: ignore[call-overload]
                channel="default",
                operation_id="some_op",
                payload=b"{}",
            )


async def test_test_client_send_unknown_operation() -> None:
    app = Repid()

    async with TestClient(app, raise_on_actor_not_found=False) as client:
        with pytest.raises(ValueError, match="Operation 'unknown_actor' not found"):
            await client.send_message(
                operation_id="unknown_actor",
                payload=b"{}",
            )


async def test_test_client_send_unknown_operation_silent() -> None:
    app = Repid()

    async with TestClient(
        app,
        raise_on_actor_not_found=False,
        raise_on_operation_not_found=False,
    ) as client:
        await client.send_message(
            operation_id="unknown_actor",
            payload=b"{}",
        )

        # We haven't found the operation, so unable to send the message
        assert len(client.get_sent_messages()) == 0


async def test_test_client_send_unknown_channel_silent() -> None:
    app = Repid()

    async with TestClient(app, raise_on_actor_not_found=False) as client:
        await client.send_message(
            channel="unknown_channel",
            payload=b"{}",
        )

        sent = client.get_sent_messages()
        assert len(sent) == 1
        assert sent[0].channel == "unknown_channel"

        processed = client.get_processed_messages()
        assert len(processed) == 0


async def test_test_client_process_next_empty_queue() -> None:
    app = Repid()

    async with TestClient(app, auto_process=False) as client:
        result = await client.process_next()
        assert result is None


async def test_test_client_messages_property_proxy() -> None:
    app = Repid()

    async with TestClient(app) as client:
        assert client.messages is app.messages


async def test_test_client_send_message_via_message_dependency() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def sender_actor(message: Message) -> None:
        await message.send_message(
            channel="target_channel",
            payload=b"sent_from_actor",
        )

    @router.actor(channel="target_channel")
    async def receiver_actor() -> None:
        pass

    app.include_router(router)
    app.messages.register_operation(operation_id="sender_actor", channel="default")

    async with TestClient(app) as client:
        await client.send_message(
            operation_id="sender_actor",
            payload=b"{}",
            headers={"topic": "sender_actor"},
        )

        # The sender_actor should have run and sent a message to target_channel
        sent = client.get_sent_messages(channel="target_channel")
        assert len(sent) == 1
        assert sent[0].payload == b"sent_from_actor"


async def test_test_client_reply_via_message_dependency() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def replier_actor(message: Message) -> None:
        await message.reply(payload=b"reply_payload")

    app.include_router(router)
    app.messages.register_operation(operation_id="replier_actor", channel="default")

    async with TestClient(app) as client:
        await client.send_message(
            operation_id="replier_actor",
            payload=b"{}",
            headers={"topic": "replier_actor"},
        )

        # The replier_actor should have run and sent a reply to the same channel
        sent = client.get_sent_messages(channel="default")
        # 1 original message + 1 reply
        assert len(sent) == 2
        assert sent[1].payload == b"reply_payload"


async def test_test_client_clear_with_messages() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(arg1: str) -> None:
        pass

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app, auto_process=False) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "first"},
            headers={"topic": "myactor"},
        )
        assert not client._message_queue.empty()

        client.clear()

        assert client._message_queue.empty()
        assert len(client.get_sent_messages()) == 0
