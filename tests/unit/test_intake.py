from __future__ import annotations

import asyncio
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from repid import Concurrency, LeaseRenewal
from repid._concurrency import _Budget
from repid._intake import _Intake
from repid.connections._submission import _channel_prefetch, _SubmissionQueue
from repid.connections.abc import Demand, ReceivedMessageT
from repid.connections.amqp import AmqpServer
from repid.connections.in_memory.message_broker import InMemorySentMessage
from repid.connections.kafka import KafkaServer
from repid.connections.kafka.subscriber import KafkaSubscriber
from repid.connections.nats import NatsServer
from repid.connections.pubsub import PubsubServer
from repid.connections.redis import RedisServer
from repid.connections.redis.message_broker import ChannelConfig, RedisSubscriber
from tests.intake import callback_intake


def _intake(
    worker: Concurrency,
    channels: dict[str, Concurrency] | None = None,
    names: tuple[str, ...] = ("a",),
) -> tuple[_Intake, list[ReceivedMessageT]]:
    submitted: list[ReceivedMessageT] = []
    intake = _Intake(_Budget(worker, channels))

    def submit(message: ReceivedMessageT) -> None:
        # Like the runner, a submitted delivery is outstanding until released.
        intake.charge(message.channel, 0)
        submitted.append(message)

    for name in names:
        intake.add_channel(name, submit)
    return intake, submitted


def _message(channel: str = "a", keep_alive_interval: float | None = None) -> Mock:
    return Mock(
        spec=ReceivedMessageT,
        channel=channel,
        payload=b"",
        keep_alive_interval=keep_alive_interval,
        keep_alive=AsyncMock(),
    )


@pytest.mark.parametrize(
    ("worker", "channels", "expected"),
    [
        pytest.param(
            Concurrency(messages=4),
            None,
            Demand(messages=4, payload_bytes=None),
            id="worker_only",
        ),
        pytest.param(
            Concurrency(),
            {"a": Concurrency(messages=2, payload_bytes=10)},
            Demand(messages=2, payload_bytes=10),
            id="channel_only",
        ),
        pytest.param(
            Concurrency(messages=3, payload_bytes=100),
            {"a": Concurrency(messages=5, payload_bytes=10)},
            Demand(messages=3, payload_bytes=10),
            id="both_take_the_tighter",
        ),
        pytest.param(
            Concurrency(),
            None,
            Demand(messages=None, payload_bytes=None),
            id="unlimited",
        ),
    ],
)
async def test_demand_combines_worker_and_channel_limits(
    worker: Concurrency,
    channels: dict[str, Concurrency] | None,
    expected: Demand,
) -> None:
    intake, _ = _intake(worker, channels)
    assert intake.channels["a"].demand == expected
    assert intake.channels["a"].limits == intake._budget.limits("a")
    assert intake.limits == worker


async def test_outstanding_deliveries_reduce_demand_at_both_scopes() -> None:
    intake, _ = _intake(
        Concurrency(messages=3, payload_bytes=100),
        {"a": Concurrency(messages=2)},
        names=("a", "b"),
    )
    intake.charge("a", 30)
    assert intake.demand == Demand(messages=2, payload_bytes=70)
    assert intake.channels["a"].demand == Demand(messages=1, payload_bytes=70)
    intake.charge("b", 80)
    # The worker's room caps every channel, so bytes run out for both.
    assert intake.channels["a"].demand == Demand(messages=1, payload_bytes=0)
    assert not intake.channels["a"].ready.done()
    assert not intake.ready.done()
    intake.release("b", 80)
    intake.release("a", 30)
    assert intake.channels["a"].demand == Demand(messages=2, payload_bytes=100)


async def test_an_empty_scope_takes_one_oversized_delivery() -> None:
    intake, _ = _intake(Concurrency(payload_bytes=10))
    channel = intake.channels["a"]
    intake.charge("a", 50)
    assert channel.demand == Demand(messages=None, payload_bytes=0)
    assert not channel.ready.done()
    intake.release("a", 50)
    assert channel.ready.done()
    assert channel.demand.payload_bytes == 10


async def test_ready_is_reused_while_there_is_room_and_resolves_when_room_returns() -> None:
    intake, _ = _intake(Concurrency(messages=1))
    channel = intake.channels["a"]
    first = channel.ready
    assert first.done()
    assert channel.ready is first

    intake.charge("a", 0)
    waiting = channel.ready
    other = channel.ready
    assert not waiting.done()
    # Each pending read is its reader's own, so cancelling one leaves the others.
    assert other is not waiting
    other.cancel()
    intake.release("a", 0)
    assert waiting.done()
    assert channel.ready is first


async def test_room_returning_resolves_only_the_affected_channel() -> None:
    intake, _ = _intake(
        Concurrency(messages=5),
        {"a": Concurrency(messages=1), "b": Concurrency(messages=1)},
        names=("a", "b"),
    )
    intake.charge("a", 0)
    intake.charge("b", 0)
    waiting_a, waiting_b = intake.channels["a"].ready, intake.channels["b"].ready
    intake.release("a", 0)
    assert waiting_a.done()
    assert not waiting_b.done()
    intake.release("b", 0)
    assert waiting_b.done()


async def test_worker_room_returning_resolves_every_channel() -> None:
    intake, _ = _intake(Concurrency(messages=1), names=("a", "b"))
    intake.charge("a", 0)
    waiting = [intake.ready, intake.channels["a"].ready, intake.channels["b"].ready]
    assert not any(future.done() for future in waiting)
    intake.release("a", 0)
    assert all(future.done() for future in waiting)


async def test_closed_intake_publishes_no_room() -> None:
    intake, _ = _intake(Concurrency(messages=2), {"a": Concurrency(messages=1)})
    channel = intake.channels["a"]
    intake.close()
    intake.close()
    assert channel.demand == Demand(messages=0, payload_bytes=0)
    assert intake.demand == Demand(messages=0, payload_bytes=0)
    waiting = channel.ready
    assert not waiting.done()
    intake.charge("a", 0)
    intake.release("a", 0)
    assert not waiting.done()


async def test_submission_queue_submits_at_once_while_there_is_room() -> None:
    intake, submitted = _intake(Concurrency(messages=1))
    queue: _SubmissionQueue[ReceivedMessageT] = _SubmissionQueue(
        intake.channels["a"],
        keep_alive=True,
    )
    message = _message(keep_alive_interval=3600)
    queue.put(message)
    assert submitted == [message]
    assert not queue


async def test_submission_queue_holds_until_room_and_hands_over_renewal() -> None:
    intake, submitted = _intake(Concurrency(messages=1))
    handed_over: list[ReceivedMessageT] = []
    queue: _SubmissionQueue[ReceivedMessageT] = _SubmissionQueue(
        intake.channels["a"],
        keep_alive=True,
        on_submitted=handed_over.append,
    )
    intake.charge("a", 0)
    first, second = _message(keep_alive_interval=3600), _message(keep_alive_interval=3600)
    queue.put(first)
    queue.put(second)
    assert len(queue) == 2
    assert not submitted

    intake.release("a", 0)
    await asyncio.sleep(0)
    # Room for one: the first is submitted, then the queue waits again.
    assert submitted == [first]
    assert handed_over == [first]
    assert len(queue) == 1


async def test_submission_queue_pauses_and_drops_stale_deliveries() -> None:
    intake, submitted = _intake(Concurrency())
    stale = _message(keep_alive_interval=3600)
    fresh = _message()
    queue: _SubmissionQueue[ReceivedMessageT] = _SubmissionQueue(
        intake.channels["a"],
        keep_alive=True,
        is_stale=lambda message: message is stale,
    )
    queue.pause()
    queue.put(stale)
    queue.put(fresh)
    assert not submitted
    queue.resume()
    assert submitted == [fresh]


async def test_submission_queue_close_returns_held_deliveries_and_stops_renewals() -> None:
    intake, submitted = _intake(Concurrency(messages=1))
    queue: _SubmissionQueue[ReceivedMessageT] = _SubmissionQueue(
        intake.channels["a"],
        keep_alive=True,
    )
    intake.charge("a", 0)
    held = _message(keep_alive_interval=3600)
    queue.put(held)
    assert queue.close() == [held]
    queue.put(_message())
    intake.release("a", 0)
    await asyncio.sleep(0)
    assert not submitted


async def test_unclaimed_renewal_is_stopped_after_submission() -> None:
    renewals: list[LeaseRenewal] = []

    def submit(message: ReceivedMessageT) -> None:
        # A worker that does not adopt the offered renewal.
        renewals.append(LeaseRenewal(message))

    intake = _Intake(_Budget(Concurrency(messages=1)))
    channel = intake.add_channel("a", submit)
    queue: _SubmissionQueue[ReceivedMessageT] = _SubmissionQueue(channel, keep_alive=True)
    intake.charge("a", 0)
    message = _message(keep_alive_interval=3600)
    queue.put(message)
    (held_renewal,) = (renewal for _, renewal in queue._held)
    assert held_renewal is not None
    intake.release("a", 0)
    await asyncio.sleep(0)
    assert renewals
    assert held_renewal._closed


@pytest.mark.parametrize(
    "make_server",
    [
        pytest.param(lambda: AmqpServer("amqp://localhost", prefetch=-1), id="amqp"),
        pytest.param(lambda: KafkaServer("localhost:9092", prefetch=-1), id="kafka"),
        pytest.param(lambda: NatsServer("nats://localhost", prefetch=-1), id="nats"),
        pytest.param(
            lambda: PubsubServer(default_project="p", prefetch_bytes=-1),
            id="pubsub",
        ),
        pytest.param(lambda: RedisServer("redis://localhost", prefetch=-1), id="redis"),
    ],
)
def test_servers_reject_negative_prefetch(make_server: Callable[[], object]) -> None:
    with pytest.raises(ValueError, match="prefetch"):
        make_server()


async def test_redis_close_returns_held_deliveries_even_if_returning_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    intake = callback_intake({"chan": AsyncMock()}, Concurrency(messages=1))
    intake.charge("chan", 0)
    client = AsyncMock()
    client.pipeline = Mock(side_effect=ConnectionError("redis down"))
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        intake=intake,
        consumer_name="consumer",
        server=MagicMock(),
    )
    await subscriber._process_stream_messages([(b"1-0", {b"payload": b"held"})], "chan", "stream")
    assert subscriber.in_flight_count == 1
    await subscriber.close()
    assert subscriber.in_flight_count == 0
    assert "message.return.error" in caplog.messages
    assert not intake.submitted


async def test_capacity_watch_is_skipped_while_there_is_room() -> None:
    redis = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        intake=callback_intake({"chan": AsyncMock()}),
        consumer_name="consumer",
        server=MagicMock(),
    )
    redis._watch_capacity("chan")
    assert not redis._capacity_watches

    kafka = KafkaSubscriber(MagicMock(), MagicMock(), callback_intake({"topic": AsyncMock()}))
    kafka._watch_capacity("topic")
    assert not kafka._capacity_watches


def test_in_memory_sent_message_keeps_its_id() -> None:
    assert InMemorySentMessage(payload=b"", message_id="id").message_id == "id"


@pytest.mark.parametrize(
    ("prefetch", "limit", "expected"),
    [
        pytest.param(None, None, 10, id="default_one_batch"),
        pytest.param(None, 3, 3, id="default_capped_by_limit"),
        pytest.param(None, 50, 10, id="default_batch_under_limit"),
        pytest.param(0, 3, 0, id="explicit_zero"),
        pytest.param(25, 3, 25, id="explicit_above_limit"),
    ],
)
def test_channel_prefetch_defaults_to_a_batch_capped_by_the_limit(
    prefetch: int | None,
    limit: int | None,
    expected: int,
) -> None:
    assert _channel_prefetch(prefetch, limit, 10) == expected


async def test_redis_default_prefetch_follows_each_channels_limit() -> None:
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={
            name: ChannelConfig(stream=name, group="group", dlq=None) for name in ("slow", "fast")
        },
        intake=callback_intake(
            {"slow": AsyncMock(), "fast": AsyncMock()},
            Concurrency(),
            {"slow": Concurrency(messages=1)},
        ),
        consumer_name="consumer",
        server=MagicMock(),
        batch_size=10,
        prefetch=None,
    )
    assert subscriber._prefetch == {"slow": 1, "fast": 10}
