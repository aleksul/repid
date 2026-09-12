import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock

import pytest

from repid.connections.pubsub.protocol.control_batcher import (
    PubsubControlBatcher,
    _AckBatch,
    _ModifyBatch,
    _Operation,
)


class _FakeProtocolClient:
    def __init__(self) -> None:
        self.acknowledge = AsyncMock()
        self.modify_ack_deadline = AsyncMock()


@pytest.fixture
def client() -> _FakeProtocolClient:
    return _FakeProtocolClient()


async def test_timer_flushes_single_ack(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await batcher.add_ack("sub1", "ack1")

    client.acknowledge.assert_awaited_once_with("sub1", ["ack1"])
    await batcher.stop()


async def test_timer_flushes_single_modify(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await batcher.add_modify_deadline("sub1", "ack1", 0)

    client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1"], 0)
    await batcher.stop()


async def test_timer_groups_concurrent_acks_into_one_rpc(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await asyncio.gather(*[batcher.add_ack("sub1", f"ack{i}") for i in range(5)])

    client.acknowledge.assert_awaited_once_with(
        "sub1",
        ["ack0", "ack1", "ack2", "ack3", "ack4"],
    )
    await batcher.stop()


async def test_concurrent_acks_of_different_subscriptions_use_separate_rpcs(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await asyncio.gather(
        batcher.add_ack("sub1", "ack1"),
        batcher.add_ack("sub2", "ack2"),
    )

    assert client.acknowledge.call_count == 2
    client.acknowledge.assert_any_await("sub1", ["ack1"])
    client.acknowledge.assert_any_await("sub2", ["ack2"])
    await batcher.stop()


async def test_max_ack_ids_triggers_immediate_flush(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    await asyncio.gather(
        batcher.add_ack("sub1", "ack1"),
        batcher.add_ack("sub1", "ack2"),
    )

    client.acknowledge.assert_awaited_once_with("sub1", ["ack1", "ack2"])
    await batcher.stop()


async def test_max_modify_ids_triggers_immediate_flush(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    await asyncio.gather(
        batcher.add_modify_deadline("sub1", "ack1", 60),
        batcher.add_modify_deadline("sub1", "ack2", 60),
    )

    client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1", "ack2"], 60)
    await batcher.stop()


async def test_ack_batch_failure_propagates_to_all_awaiters(client: _FakeProtocolClient) -> None:
    client.acknowledge.side_effect = RuntimeError("nack failed")
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    with pytest.raises(RuntimeError, match="nack failed"):
        await asyncio.gather(
            batcher.add_ack("sub1", "ack1"),
            batcher.add_ack("sub1", "ack2"),
        )

    await batcher.stop()


async def test_modify_batch_failure_propagates_to_all_awaiters(client: _FakeProtocolClient) -> None:
    client.modify_ack_deadline.side_effect = RuntimeError("modify failed")
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    with pytest.raises(RuntimeError, match="modify failed"):
        await asyncio.gather(
            batcher.add_modify_deadline("sub1", "ack1", 0),
            batcher.add_modify_deadline("sub1", "ack2", 0),
        )

    await batcher.stop()


async def test_cancelling_caller_of_full_ack_batch_does_not_abandon_other_awaiters(
    client: _FakeProtocolClient,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def acknowledge(_subscription: str, _ack_ids: list[str]) -> None:
        started.set()
        await release.wait()

    client.acknowledge.side_effect = acknowledge
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    first = asyncio.create_task(batcher.add_ack("sub1", "ack1"))
    await asyncio.sleep(0)
    final = asyncio.create_task(batcher.add_ack("sub1", "ack2"))

    await started.wait()
    final.cancel()
    with suppress(asyncio.CancelledError):
        await final

    release.set()
    await asyncio.wait_for(first, timeout=1)

    client.acknowledge.assert_awaited_once_with("sub1", ["ack1", "ack2"])
    await batcher.stop()


async def test_stop_waits_for_blocked_immediate_batch(client: _FakeProtocolClient) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def acknowledge(_subscription: str, _ack_ids: list[str]) -> None:
        started.set()
        await release.wait()

    client.acknowledge.side_effect = acknowledge
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=1)
    await batcher.start()

    add_task = asyncio.create_task(batcher.add_ack("sub1", "ack1"))
    await started.wait()
    stop_task = asyncio.create_task(batcher.stop())
    await asyncio.sleep(0)
    assert not stop_task.done()

    release.set()
    await stop_task
    await add_task


@pytest.mark.parametrize("error", [RuntimeError("RPC failed"), asyncio.CancelledError()])
async def test_abandoned_immediate_future_consumes_rpc_failure(
    client: _FakeProtocolClient,
    error: BaseException,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def record_unhandled(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        unhandled.append(context)

    async def acknowledge(_subscription: str, _ack_ids: list[str]) -> None:
        started.set()
        await release.wait()
        raise error

    client.acknowledge.side_effect = acknowledge
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=1)
    await batcher.start()
    loop.set_exception_handler(record_unhandled)
    try:
        task = asyncio.create_task(batcher.add_ack("sub1", "ack1"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        release.set()
        await batcher.stop()
        await asyncio.sleep(0)
        assert not unhandled
    finally:
        loop.set_exception_handler(previous_handler)


async def test_cancelling_caller_of_full_modify_batch_does_not_abandon_other_awaiters(
    client: _FakeProtocolClient,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def modify(
        _subscription: str,
        _ack_ids: list[str],
        _seconds: int,
    ) -> None:
        started.set()
        await release.wait()

    client.modify_ack_deadline.side_effect = modify
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()

    first = asyncio.create_task(batcher.add_modify_deadline("sub1", "ack1", 60))
    await asyncio.sleep(0)
    final = asyncio.create_task(batcher.add_modify_deadline("sub1", "ack2", 60))

    await started.wait()
    final.cancel()
    with suppress(asyncio.CancelledError):
        await final

    release.set()
    await asyncio.wait_for(first, timeout=1)

    client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1", "ack2"], 60)
    await batcher.stop()


async def test_concurrent_modifies_with_different_seconds_use_separate_rpcs(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await asyncio.gather(
        batcher.add_modify_deadline("sub1", "ack1", 0),
        batcher.add_modify_deadline("sub1", "ack2", 1),
    )

    assert client.modify_ack_deadline.call_count == 2
    client.modify_ack_deadline.assert_any_await("sub1", ["ack1"], 0)
    client.modify_ack_deadline.assert_any_await("sub1", ["ack2"], 1)
    await batcher.stop()


async def test_concurrent_modifies_with_same_seconds_are_batched_together(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()

    await asyncio.gather(
        batcher.add_modify_deadline("sub1", "ack1", 0),
        batcher.add_modify_deadline("sub1", "ack2", 0),
    )

    client.modify_ack_deadline.assert_awaited_once_with("sub1", ["ack1", "ack2"], 0)
    await batcher.stop()


async def test_stop_flushes_pending_acks(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=100)
    await batcher.start()

    task = asyncio.create_task(batcher.add_ack("sub1", "ack1"))

    await batcher.stop()

    client.acknowledge.assert_awaited_once_with("sub1", ["ack1"])
    await task


async def test_stop_is_idempotent(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=100)
    await batcher.start()
    await batcher.stop()
    await batcher.stop()


async def test_ack_execute_cancelled_error_resolves_all_futures(
    client: _FakeProtocolClient,
) -> None:
    client.acknowledge.side_effect = asyncio.CancelledError()
    batcher = PubsubControlBatcher(client)
    batch = _AckBatch()
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    batch.operations.append(_Operation("ack1", future))

    with pytest.raises(asyncio.CancelledError):
        await batcher._execute_ack_batch("sub1", batch)

    with pytest.raises(asyncio.CancelledError):
        await future


async def test_modify_execute_cancelled_error_resolves_all_futures(
    client: _FakeProtocolClient,
) -> None:
    client.modify_ack_deadline.side_effect = asyncio.CancelledError()
    batcher = PubsubControlBatcher(client)
    batch = _ModifyBatch(60)
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    batch.operations.append(_Operation("ack1", future))

    with pytest.raises(asyncio.CancelledError):
        await batcher._execute_modify_batch("sub1", batch)

    with pytest.raises(asyncio.CancelledError):
        await future


async def test_add_after_stop_hangs_indefinitely(client: _FakeProtocolClient) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=0.01, max_batch_ids=100)
    await batcher.start()
    await batcher.stop()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(batcher.add_ack("sub1", "ack1"), timeout=0.1)


async def test_relay_cancels_outer_when_operation_future_already_cancelled(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client)
    loop = asyncio.get_running_loop()
    future: asyncio.Future[None] = loop.create_future()
    future.cancel()
    operation = _Operation("ack1", future)

    task = asyncio.create_task(batcher._await_operation(operation))

    # On Python 3.10 the task's CancelledError-subclass outcome is reported as
    # a plain CancelledError to the awaiting side.
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancelling_caller_withdraws_pending_modify_operation(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client, flush_interval=100, max_batch_ids=100)
    await batcher.start()

    task = asyncio.create_task(batcher.add_modify_deadline("sub1", "ack1", 60))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    client.modify_ack_deadline.assert_not_awaited()
    await batcher.stop()


async def test_cancel_colliding_with_batch_failure_consumes_batch_error(
    client: _FakeProtocolClient,
) -> None:
    batcher = PubsubControlBatcher(client)
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def record_unhandled(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        unhandled.append(context)

    future: asyncio.Future[None] = loop.create_future()
    operation = _Operation("ack1", future)

    task = asyncio.create_task(batcher._await_operation(operation))
    await asyncio.sleep(0)

    # The batch fails, and the caller is cancelled before it gets to process
    # the failure: the failure must be consumed, not reported as unhandled.
    future.set_exception(RuntimeError("rpc failed"))
    await asyncio.sleep(0)
    task.cancel()

    loop.set_exception_handler(record_unhandled)
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
        await batcher.stop()
        await asyncio.sleep(0)
        assert not unhandled
    finally:
        loop.set_exception_handler(previous_handler)
