# pylint: disable=redefined-outer-name
import time
import uuid

import pytest
from redis.asyncio import Redis

from repid.connections.redis_conn import RedisMessaging
from repid.data import Message

pytestmark = pytest.mark.anyio


@pytest.fixture()
def redis_messaging(redis: Redis) -> RedisMessaging:
    return RedisMessaging(redis)


def message_fabric() -> Message:
    return Message(
        id_=uuid.uuid4().hex,
        topic="test_actor",
        queue="test",
        timestamp=int(time.time()),
    )


async def test_enqueue(redis_messaging: RedisMessaging):
    message = message_fabric()

    await redis_messaging.enqueue(message)
    consumed = await redis_messaging.consume(queue_name=message.queue)
    assert consumed == message
