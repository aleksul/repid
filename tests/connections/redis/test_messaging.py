import hypothesis
import hypothesis.strategies
import pytest
from redis.asyncio import Redis

from repid.connections.redis_conn import RedisMessaging
from repid.data import Message

pytestmark = pytest.mark.anyio


@pytest.fixture()
def redis_messaging(redis: Redis) -> RedisMessaging:
    return RedisMessaging(redis)


MESSAGE = hypothesis.strategies.builds(Message)


@hypothesis.given(msg=MESSAGE)
@hypothesis.settings(suppress_health_check=(hypothesis.HealthCheck.function_scoped_fixture,))
async def test_enqueue(redis_messaging: RedisMessaging, msg: Message):
    await redis_messaging.enqueue(msg)
    consumed = await redis_messaging.consume(queue_name=msg.queue)
    assert consumed == msg
