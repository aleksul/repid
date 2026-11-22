import asyncio

from repid.connections.in_memory.utils import DummyQueue


async def test_dummy_queue_message_uses_stable_delivery_identity() -> None:
    msg1 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")
    msg2 = DummyQueue.Message(payload=b"1", headers={"a": "b"}, content_type="json", message_id="1")

    deliveries = {msg1, msg2}
    msg1.headers["a"] = "changed"  # pyrefly: ignore[unsupported-operation]

    assert msg1 is not msg2
    assert msg1 != msg2
    assert len(deliveries) == 2
    assert msg1 in deliveries


async def test_resumed_reader_waits_if_another_reader_took_the_message() -> None:
    queue = DummyQueue().queue
    resumed = asyncio.Event()
    first = DummyQueue.Message(payload=b"first")
    next_message = DummyQueue.Message(payload=b"next")
    queue.put_nowait(first)
    waiting = asyncio.create_task(queue.get_when_resumed(resumed))
    try:
        await asyncio.sleep(0)
        assert queue.get_nowait() is first
        resumed.set()
        await asyncio.sleep(0)
        assert not waiting.done()
        queue.put_nowait(next_message)
        assert await asyncio.wait_for(waiting, 1) is next_message
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
