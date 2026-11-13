import asyncio
import uuid
from contextlib import suppress
from typing import cast
from unittest.mock import patch
from urllib.parse import urlsplit

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from repid.connections.abc import ReceivedMessageT
from repid.connections.redis import RedisServer
from repid.connections.redis.message_broker import ChannelConfig, RedisSubscriber

pytestmark = pytest.mark.integration_target(name="redis")


@pytest.mark.parametrize("block_ms", [0, 30000])
async def test_close_interrupts_notifications_without_admin_permissions(
    redis_connection: RedisServer,
    block_ms: int,
) -> None:
    username = f"worker-{uuid.uuid4().hex}"
    address = urlsplit(redis_connection._dsn)
    server = RedisServer(
        f"redis://{username}:test@{address.hostname}:{address.port}/0",
        block_ms=block_ms,
    )
    channels = [f"shutdown-{uuid.uuid4().hex}" for _ in range(2)]

    async def callback(_message: ReceivedMessageT) -> None:
        pytest.fail("Empty streams must not deliver messages")

    async with redis_connection.connection():
        admin = redis_connection._redis
        assert admin is not None
        await admin.execute_command(
            "ACL",
            "SETUSER",
            username,
            "on",
            ">test",
            "~*",
            "+@stream",
            "+ping",
            "+multi",
            "+exec",
        )
        try:
            async with server.connection():
                subscriber = cast(
                    RedisSubscriber,
                    await server.subscribe(channels_to_callbacks=dict.fromkeys(channels, callback)),
                )

                async def wait_for_blocked_notifications() -> None:
                    assert admin is not None
                    while True:
                        blocked = [
                            client
                            for client in await admin.client_list()
                            if client["cmd"] == "xread" and client["user"] == username
                        ]
                        if len(blocked) == len(channels):
                            return
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_for_blocked_notifications(), timeout=2)
                await subscriber.pause()
                await asyncio.wait_for(subscriber.close(), timeout=1)
                assert subscriber.task.done()
                assert not subscriber._notification_tasks
        finally:
            await admin.acl_deluser(username)
            await admin.delete(*(server.stream_name_for(channel) for channel in channels))


@pytest.mark.parametrize("closing", [False, True])
async def test_recover_assigned_messages_after_lost_read_reply(
    redis_connection: RedisServer,
    closing: bool,
) -> None:
    server = RedisServer(
        redis_connection._dsn,
        batch_size=1,
        read_timeout=0.05,
        retry_delay=0.001,
        claim_interval=0,
        block_ms=0,
    )
    channel = f"lost-reply-{uuid.uuid4().hex}"
    assigned = asyncio.Event()
    complete = asyncio.Event()
    delivered: list[bytes] = []
    active: list[ReceivedMessageT] = []
    lose_reply = True

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.payload)
        if message.payload == b"active":
            active.append(message)
            return  # The runner likewise returns its callback while the actor is running.
        await message.ack()
        if len(delivered) == (1 if closing else 3):
            complete.set()

    async with server.connection():
        client = server._redis
        assert client is not None
        stream = server.stream_name_for(channel)
        payloads = [b"lost"] if closing else [b"active", b"lost", b"new"]
        for payload in payloads:
            await client.xadd(stream, {b"payload": payload})
        original = client.xreadgroup

        async def read_group(
            groupname: str,
            consumername: str,
            streams: dict[str, str],
            count: int,
        ) -> object:
            nonlocal lose_reply
            result = await original(
                groupname=groupname,
                consumername=consumername,
                streams=streams,  # pyrefly: ignore[bad-argument-type]
                count=count,
            )
            if (
                lose_reply
                and result
                and all(cursor == ">" for cursor in streams.values())
                and result[0][1][0][1][b"payload"] == b"lost"
            ):
                lose_reply = False
                assigned.set()
                await asyncio.Future[None]()
            return cast("object", result)

        try:
            with patch.object(client, "xreadgroup", side_effect=read_group):
                subscriber = cast(
                    RedisSubscriber,
                    await server.subscribe(
                        channels_to_callbacks={channel: callback},
                    ),
                )
                await asyncio.wait_for(assigned.wait(), timeout=1)
                if closing:
                    await asyncio.wait_for(subscriber.close(), timeout=1)
                    assert not delivered
                    subscriber = cast(
                        RedisSubscriber,
                        await server.subscribe(
                            channels_to_callbacks={channel: callback},
                        ),
                    )
                await asyncio.wait_for(complete.wait(), timeout=1)
                assert sorted(delivered) == sorted(payloads)
                for message in active:
                    await message.ack()
                await asyncio.wait_for(subscriber.close(), timeout=1)
                assert not subscriber._delivered_messages
                assert (await client.xpending(stream, f"repid:{channel}:group"))["pending"] == 0
        finally:
            await client.delete(stream)


async def test_recovery_does_not_redeliver_a_concurrently_acknowledged_claim(
    redis_connection: RedisServer,
) -> None:
    server = RedisServer(redis_connection._dsn)
    channel = f"recovery-race-{uuid.uuid4().hex}"
    delivered: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        await message.ack()

    async with server.connection():
        client = server._redis
        assert client is not None
        stream = server.stream_name_for(channel)
        cfg = ChannelConfig(stream=stream, group=f"repid:{channel}:group", dlq=None)
        subscriber = RedisSubscriber(
            redis_client=client,
            channels={channel: cfg},
            callbacks={channel: callback},
            consumer_name="recovery-race",
            concurrency_limit=None,
            server=server,
            min_idle_ms=0,
        )
        try:
            await client.xgroup_create(stream, cfg.group, id="0", mkstream=True)
            message_id = await client.xadd(stream, {b"payload": b"job"})
            await client.xreadgroup(cfg.group, "recovery-race", {stream: ">"}, count=1)
            original_range = client.xrange

            async def range_then_claim(name: str, **kwargs: object) -> object:
                result = await original_range(
                    name,
                    min=cast("bytes | str", kwargs["min"]),
                    max=cast("bytes | str", kwargs["max"]),
                    count=1,
                )
                # The payload is already on its way when stale reclaim settles it.
                await subscriber._reclaim_pending(cfg, channel, callback)
                await asyncio.gather(*subscriber._callback_tasks)
                assert (await client.xpending(stream, cfg.group))["pending"] == 0
                return cast("object", result)

            with patch.object(client, "xrange", side_effect=range_then_claim):
                await subscriber._recover_pending(cfg.group, subscriber._channels)
                await asyncio.gather(*subscriber._callback_tasks)
            assert delivered == [message_id.decode()]
            assert not subscriber._recovery_deliveries
        finally:
            await subscriber.close()
            await client.delete(stream)


@pytest.mark.parametrize("first_result", ["manual", "ack_failure"])
@pytest.mark.parametrize("uncertain_read", [False, True])
async def test_stale_reclaim_retries_completed_unsettled_delivery(
    redis_connection: RedisServer,
    first_result: str,
    uncertain_read: bool,
) -> None:
    server = RedisServer(
        redis_connection._dsn,
        claim_interval=0.01,
        min_idle_ms=150,
        retry_delay=0.005,
        block_ms=0,
    )
    channel = f"stale-retry-{uuid.uuid4().hex}"
    calls = 0
    complete = asyncio.Event()
    ack_attempts = 0
    failed_read = False

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            if first_result == "ack_failure":
                with suppress(RedisConnectionError):
                    await message.ack()
            return
        await message.ack()
        complete.set()

    async with server.connection():
        client = server._redis
        assert client is not None
        original_ack = client.xack
        original_read = client.xreadgroup

        async def read_group(
            groupname: str,
            consumername: str,
            streams: dict[str, str],
            count: int,
        ) -> object:
            nonlocal failed_read
            if uncertain_read and calls == 1 and not failed_read:
                failed_read = True
                raise RedisConnectionError("Uncertain read after an unsettled delivery")
            return cast(
                "object",
                await original_read(
                    groupname=groupname,
                    consumername=consumername,
                    streams=streams,  # pyrefly: ignore[bad-argument-type]
                    count=count,
                ),
            )

        async def ack(name: str, groupname: str, *ids: str) -> int:
            nonlocal ack_attempts
            ack_attempts += 1
            if first_result == "ack_failure" and ack_attempts == 1:
                raise RedisConnectionError("Transient ack failure")
            return cast("int", await original_ack(name, groupname, *ids))

        stream = server.stream_name_for(channel)
        await client.xadd(stream, {b"payload": b"retry"})
        try:
            with (
                patch.object(client, "xack", side_effect=ack),
                patch.object(client, "xreadgroup", side_effect=read_group),
            ):
                subscriber = cast(
                    RedisSubscriber,
                    await server.subscribe(
                        channels_to_callbacks={channel: callback},
                    ),
                )
                await asyncio.wait_for(complete.wait(), timeout=1)
                await asyncio.wait_for(subscriber.close(), timeout=1)
                assert calls == 2
                assert failed_read is uncertain_read
                assert not subscriber._delivered_messages
                assert (await client.xpending(stream, f"repid:{channel}:group"))["pending"] == 0
        finally:
            await client.delete(stream)
