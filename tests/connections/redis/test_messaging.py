import time
import uuid

import pytest
from redis.asyncio import Redis

from repid.connections.redis_conn import RedisMessaging
from repid.data import Message, AnyMessageT

pytestmark = pytest.mark.anyio


@pytest.fixture()
def redis_messaging(redis_service: str, redis: Redis) -> RedisMessaging:
    r = RedisMessaging(redis_service)
    r.conn = redis
    return r


def message_fabric() -> Message:
    return Message(id_=uuid.uuid4().hex, actor_name="test_actor", queue="test")

async def is_enqueued(redis: Redis, message: AnyMessageT) -> bool:
    pass


async def test_enqueue(redis: Redis, redis_messaging: RedisMessaging):
    message = message_fabric()
    await redis_messaging.enqueue(message)
    consumed = await redis_messaging.consume(queue_name=message.queue)
    assert consumed == message
