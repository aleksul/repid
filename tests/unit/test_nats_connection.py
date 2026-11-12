from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig
from nats.js.errors import APIError, NotFoundError
from nats.js.errors import Error as JetStreamError

from repid import Concurrency, LeaseRenewal, Repid, Router
from repid.connections.nats.message_broker import (
    NatsReceivedMessage,
    NatsServer,
    _delivery_ack_wait,
)


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_failure_clears_client_and_can_retry(
    mock_connect: AsyncMock,
) -> None:
    connected_client = MagicMock(is_connected=True)
    mock_connect.side_effect = [ConnectionError("offline"), connected_client]
    server = NatsServer("nats://localhost:4222")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert not server.is_connected
    assert server._nc is None
    assert server._js is None

    await server.connect()

    assert server.is_connected
    assert server._nc is connected_client
    assert server._js is connected_client.jetstream.return_value
    assert mock_connect.call_count == 2


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_closes_stale_client_before_reconnect(
    mock_connect: AsyncMock,
) -> None:
    stale_client = MagicMock(is_connected=False)
    stale_client.close = AsyncMock()
    server = NatsServer("nats://localhost:4222")
    server._nc = stale_client

    connected_client = MagicMock(is_connected=True)
    mock_connect.return_value = connected_client

    await server.connect()

    stale_client.close.assert_awaited_once()
    assert server._nc is connected_client
    assert server._js is connected_client.jetstream.return_value


@patch("nats.connect", new_callable=AsyncMock)
async def test_nats_connect_failure_with_stale_client_closes_it(
    mock_connect: AsyncMock,
) -> None:
    stale_client = MagicMock(is_connected=False)
    stale_client.close = AsyncMock()
    server = NatsServer("nats://localhost:4222")
    server._nc = stale_client
    mock_connect.side_effect = ConnectionError("offline")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    stale_client.close.assert_awaited_once()
    assert server._nc is None
    assert server._js is None
    assert not server.is_connected


def _make_nats_received_message(msg: MagicMock) -> NatsReceivedMessage:
    return NatsReceivedMessage(msg, NatsServer("nats://localhost:4222"), "chan")


@pytest.mark.parametrize(("ack_wait", "interval"), [(None, None), (0.75, 0.25), (3.0, 1.0)])
def test_nats_keep_alive_interval(ack_wait: float | None, interval: float | None) -> None:
    server = NatsServer("nats://localhost:4222")
    message = NatsReceivedMessage(MagicMock(), server, "chan", ack_wait=ack_wait)
    assert message.keep_alive_interval == interval


@pytest.mark.parametrize(("reply", "ack_wait"), [("", 3), ("$JS.ACK.s.c.3.1.1.0.0", 0.3)])
def test_nats_retry_uses_delivery_specific_backoff(reply: str, ack_wait: float) -> None:
    config = ConsumerConfig(ack_wait=3, backoff=[3, 0.3])
    assert _delivery_ack_wait(config, Msg(MagicMock(), reply=reply)) == ack_wait


async def test_nats_received_message_ack_failure_resets_action() -> None:
    msg = MagicMock()
    msg.ack = AsyncMock(side_effect=RuntimeError("ack failed"))
    received = _make_nats_received_message(msg)

    with pytest.raises(RuntimeError, match="ack failed"):
        await received.ack()

    assert received.action is None
    assert not received.is_acted_on


async def test_nats_received_message_reject_failure_resets_action() -> None:
    msg = MagicMock()
    msg.nak = AsyncMock(side_effect=RuntimeError("nak failed"))
    received = _make_nats_received_message(msg)

    with pytest.raises(RuntimeError, match="nak failed"):
        await received.reject()

    assert received.action is None
    assert not received.is_acted_on


def _server_with_durable(ack_wait: float = 30) -> tuple[NatsServer, MagicMock, MagicMock]:
    server = NatsServer("nats://localhost:4222")
    server._nc = MagicMock(is_connected=True)

    def consumer_info(stream: str, durable: str) -> MagicMock:
        info = MagicMock(
            push_bound=True,
            config=ConsumerConfig(
                max_ack_pending=7,
                ack_wait=ack_wait,
                deliver_group=durable,
                deliver_subject=f"inbox.{stream}.{durable}",
            ),
        )
        info.name = durable
        return info

    sub = MagicMock(unsubscribe=AsyncMock())
    js = MagicMock(
        find_stream_name_by_subject=AsyncMock(return_value="stream"),
        consumer_info=AsyncMock(side_effect=consumer_info),
        subscribe_bind=AsyncMock(return_value=sub),
        add_consumer=AsyncMock(),
    )
    server._js = js
    return server, js, sub


async def test_nats_subscription_keeps_durable_configuration() -> None:
    server, js, _ = _server_with_durable()
    delivered = asyncio.Event()
    callback = AsyncMock(side_effect=lambda _: delivered.set())  # pyrefly: ignore[implicit-any-lambda]
    subscriber = await server.subscribe(
        channel_concurrency={"subject": Concurrency(messages=3)},
        channels_to_callbacks={"subject": callback, "other": callback},
        concurrency=Concurrency(messages=1),
    )
    async with asyncio.timeout(1):
        while js.subscribe_bind.await_count < 2:
            await asyncio.sleep(0)
    js.add_consumer.assert_not_awaited()
    assert js.consumer_info.await_count == 2
    assert all(
        call.kwargs["config"].max_ack_pending == 7 for call in js.subscribe_bind.await_args_list
    )
    raw = MagicMock(data=b"{}", headers=None, reply="")
    await js.subscribe_bind.call_args.kwargs["cb"](raw)
    await asyncio.wait_for(delivered.wait(), 1)
    callback.assert_awaited_once()
    await subscriber.close()


async def test_nats_does_not_bind_when_metadata_lookup_fails() -> None:
    server, js, _ = _server_with_durable()
    js.consumer_info.side_effect = TimeoutError("metadata unavailable")

    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"subject": AsyncMock()},
    )
    with pytest.raises(TimeoutError, match="metadata unavailable"):
        await subscriber.task
    js.subscribe_bind.assert_not_awaited()


async def test_nats_does_not_bind_a_nonmatching_durable() -> None:
    server, js, _ = _server_with_durable()
    js.consumer_info.side_effect = None
    js.consumer_info.return_value.config = ConsumerConfig(deliver_group="other")
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    with pytest.raises(JetStreamError, match="does not match the expected push queue"):
        await subscriber.task
    js.subscribe_bind.assert_not_awaited()


@pytest.mark.parametrize("err_code", [10013, 10105, 10148, 10121])
async def test_nats_recovers_only_durable_create_conflicts(err_code: int) -> None:
    server, js, sub = _server_with_durable()
    existing = js.consumer_info.side_effect("stream", "jobs_group")
    js.consumer_info.side_effect = [NotFoundError(), existing]
    js.add_consumer.side_effect = APIError(err_code=err_code)
    binding = asyncio.Event()

    def bind(**_: Any) -> MagicMock:
        binding.set()
        return sub

    js.subscribe_bind.side_effect = bind
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    try:
        if err_code == 10121:
            with pytest.raises(APIError, match="10121"):
                await subscriber.task
            js.subscribe_bind.assert_not_awaited()
        else:
            await asyncio.wait_for(binding.wait(), 1)
            assert js.consumer_info.await_count == 2
            assert js.subscribe_bind.call_args.kwargs["config"] is existing.config
    finally:
        await subscriber.close()


async def test_nats_rebinds_if_concurrent_creation_replaced_the_inbox() -> None:
    server, js, first_sub = _server_with_durable()
    original = js.consumer_info.side_effect("stream", "jobs_group")
    original.push_bound = False
    winner = js.consumer_info.side_effect("stream", "jobs_group")
    winner.config.deliver_subject = "winner"
    js.consumer_info.side_effect = [NotFoundError(), winner]
    js.add_consumer.return_value = original
    second_sub = MagicMock(unsubscribe=AsyncMock())
    js.subscribe_bind.side_effect = [first_sub, second_sub]
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    try:
        async with asyncio.timeout(1):
            while js.subscribe_bind.await_count < 2:
                await asyncio.sleep(0)
        first_sub.unsubscribe.assert_awaited_once()
        assert js.subscribe_bind.call_args.kwargs["config"] is winner.config
    finally:
        await subscriber.close()
    second_sub.unsubscribe.assert_awaited_once()


@pytest.mark.parametrize("metadata_error", [None, TimeoutError("stale lookup")])
async def test_nats_pause_resume_invalidates_startup_metadata(
    metadata_error: Exception | None,
) -> None:
    server, js, sub = _server_with_durable()
    original_consumer_info: Callable[[str, str], MagicMock] = js.consumer_info.side_effect
    lookup_started = asyncio.Event()
    release_lookup = asyncio.Event()

    async def lookup(stream: str, durable: str) -> MagicMock:
        if not lookup_started.is_set():
            lookup_started.set()
            await release_lookup.wait()
            if metadata_error is not None:
                raise metadata_error
        return original_consumer_info(stream, durable)

    js.consumer_info.side_effect = lookup
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    try:
        await asyncio.wait_for(lookup_started.wait(), 1)
        await subscriber.pause()
        await subscriber.resume()
        release_lookup.set()
        await asyncio.sleep(0)
        assert subscriber.is_active
        js.subscribe_bind.assert_awaited_once()
        sub.unsubscribe.assert_not_awaited()
    finally:
        release_lookup.set()
        await subscriber.close()
    sub.unsubscribe.assert_awaited_once()


async def test_nats_can_pause_before_starting_lookup() -> None:
    server, js, _ = _server_with_durable()
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    try:
        await subscriber.pause()
        await asyncio.sleep(0)
        js.consumer_info.assert_not_awaited()
    finally:
        await subscriber.close()


async def test_nats_unsubscribes_if_paused_while_subscription_is_opening() -> None:
    server, js, sub = _server_with_durable()
    opening = asyncio.Event()
    release = asyncio.Event()
    unsubscribed = asyncio.Event()
    sub.unsubscribe.side_effect = unsubscribed.set

    async def open_subscription(*_: Any, **__: Any) -> MagicMock:
        opening.set()
        await release.wait()
        return sub

    js.subscribe_bind.side_effect = open_subscription
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"subject": AsyncMock()},
    )
    try:
        await asyncio.wait_for(opening.wait(), 1)
        await subscriber.pause()
        release.set()
        await asyncio.wait_for(unsubscribed.wait(), 1)
        assert not subscriber.is_active
    finally:
        release.set()
        await subscriber.close()


@pytest.mark.parametrize("fail_binding", [False, True])
async def test_nats_close_cleans_up_binding_in_progress(fail_binding: bool) -> None:
    server, js, sub = _server_with_durable()
    opening = asyncio.Event()
    release_bind = asyncio.Event()

    async def bind(**_: Any) -> MagicMock:
        opening.set()
        await release_bind.wait()
        if fail_binding:
            raise ConnectionError("binding failed")
        return sub

    js.subscribe_bind.side_effect = bind
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": AsyncMock()},
    )
    await asyncio.wait_for(opening.wait(), 1)
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    try:
        closing = asyncio.create_task(subscriber.close())
        await asyncio.sleep(0)
        release_bind.set()
        await asyncio.wait_for(closing, 1)
        await asyncio.sleep(0)
        if fail_binding:
            sub.unsubscribe.assert_not_awaited()
        else:
            sub.unsubscribe.assert_awaited_once()
        assert not errors
    finally:
        release_bind.set()
        loop.set_exception_handler(previous_handler)
        await subscriber.close()


async def test_nats_renews_all_deliveries_while_waiting_for_admission() -> None:
    server, js, sub = _server_with_durable(ack_wait=0.3)
    opened = asyncio.Event()
    release_actor = asyncio.Event()

    async def open_subscription(*_: Any, **__: Any) -> MagicMock:
        opened.set()
        return sub

    js.subscribe_bind.side_effect = open_subscription
    app = Repid()
    app.servers.register_server("default", server, is_default=True)
    router = Router()

    @router.actor(channel="jobs")
    async def job() -> None:
        await release_actor.wait()

    app.include_router(router)
    keep_alive_called = [asyncio.Event() for _ in range(3)]
    deliveries = [
        MagicMock(
            data=b"{}",
            headers={"topic": "job", "content-type": "application/json"},
            reply="",
            in_progress=AsyncMock(side_effect=event.set),
            ack=AsyncMock(),
            nak=AsyncMock(),
        )
        for event in keep_alive_called
    ]
    worker = asyncio.create_task(
        app.run_worker(
            concurrency=Concurrency(payload_bytes=2),
            messages_limit=3,
            graceful_shutdown_time=0.01,
            register_signals=[],
        ),
    )
    try:
        await asyncio.wait_for(opened.wait(), 1)
        handler = js.subscribe_bind.call_args.kwargs["cb"]
        for delivery in deliveries:
            await asyncio.wait_for(handler(delivery), 1)
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in keep_alive_called)),
            1,
        )
        release_actor.set()
        result = await asyncio.wait_for(asyncio.shield(worker), 1)
        assert result.processed == 3
        for delivery in deliveries:
            delivery.ack.assert_awaited_once()
            delivery.nak.assert_not_awaited()
    finally:
        release_actor.set()
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)


async def test_nats_pause_stops_intake_and_close_returns_unfinished_submissions() -> None:
    server, js, sub = _server_with_durable()
    opened = asyncio.Event()
    submitting = asyncio.Event()

    async def open_subscription(*_: Any, **__: Any) -> MagicMock:
        opened.set()
        return sub

    async def blocked(_message: Any, _renewal: LeaseRenewal | None = None) -> None:
        submitting.set()
        await asyncio.Event().wait()

    js.subscribe_bind.side_effect = open_subscription
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": blocked},
    )
    delivery = MagicMock(data=b"{}", headers=None, reply="", nak=AsyncMock())
    late = MagicMock(data=b"{}", headers=None, reply="", nak=AsyncMock())
    try:
        await asyncio.wait_for(opened.wait(), 1)
        handler = js.subscribe_bind.call_args.kwargs["cb"]
        await handler(delivery)
        await asyncio.wait_for(submitting.wait(), 1)
        await subscriber.pause()
        await handler(late)
        late.nak.assert_awaited_once()
        delivery.nak.assert_not_awaited()
    finally:
        await subscriber.close()
    delivery.nak.assert_awaited_once()
    sub.unsubscribe.assert_awaited_once()


@pytest.mark.parametrize("close", [False, True])
async def test_nats_stopped_before_submission_starts(close: bool) -> None:
    server, js, _ = _server_with_durable()
    callback = AsyncMock()
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": callback},
    )
    rejected = asyncio.Event()
    raw = MagicMock(nak=AsyncMock(side_effect=rejected.set))
    try:
        async with asyncio.timeout(1):
            while not js.subscribe_bind.await_count:
                await asyncio.sleep(0)
            handler = js.subscribe_bind.call_args.kwargs["cb"]
            await handler(raw)
            if close:
                await subscriber.close()
            else:
                await subscriber.pause()
            await rejected.wait()
        callback.assert_not_awaited()
        raw.nak.assert_awaited_once()
    finally:
        await subscriber.close()


async def test_nats_reports_failed_callback_settlement(caplog: pytest.LogCaptureFixture) -> None:
    server, js, _ = _server_with_durable()
    server._dlq_topic_strategy = None
    callback = AsyncMock(side_effect=ValueError("callback failed"))
    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"jobs": callback},
    )
    raw = MagicMock(term=AsyncMock(side_effect=RuntimeError("settlement failed")))
    try:
        async with asyncio.timeout(1):
            while not js.subscribe_bind.await_count:
                await asyncio.sleep(0)
            await js.subscribe_bind.call_args.kwargs["cb"](raw)
            while "consumer.error.settlement" not in caplog.messages:
                await asyncio.sleep(0)
        raw.term.assert_awaited_once()
    finally:
        await subscriber.close()
