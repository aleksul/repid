from typing import Dict, Type

from ..connection import Bucketing, Messaging
from .amqp_conn import RabbitMessaging
from .redis_conn import RedisBucketing

# from .kafka_conn import KafkaMessaging
# from .nats_conn import NatsMessaging
# from .redis_conn import RedisBucketing, RedisMessaging

CONNECTIONS: Dict[str, Type[Messaging]] = {
    #    "redis://": RedisMessaging,
    #    "rediss://": RedisMessaging,
    #    "kafka://": KafkaMessaging,
    "amqp://": RabbitMessaging,  # type: ignore[dict-item]
    "amqps://": RabbitMessaging,  # type: ignore[dict-item]
    #   "nats://": NatsMessaging,
}

BUCKETINGS: Dict[str, Type[Bucketing]] = {
    "redis://": RedisBucketing,
    "rediss://": RedisBucketing,
}


def _get_messaging_from_string(dsn: str) -> Messaging:
    global CONNECTIONS
    for prefix, conn in CONNECTIONS.items():
        if dsn.startswith(prefix):
            return conn(dsn)
    raise ValueError(f"Unsupported DSN: {dsn}")


def _get_bucketing_from_string(dsn: str) -> Bucketing:
    global BUCKETINGS
    for prefix, bucketing in BUCKETINGS.items():
        if dsn.startswith(prefix):
            return bucketing(dsn)
    raise ValueError(f"Unsupported DSN: {dsn}")
