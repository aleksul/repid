from typing import Dict, Type

from ..connection import Bucketing, Messaging
from .rabbitmq_messaging import RabbitMessaging
from .redis import RedisBucketing, RedisMessaging

# from .kafka_conn import KafkaMessaging
# from .nats_conn import NatsMessaging

CONNECTIONS: Dict[str, Type[Messaging]] = {
    "amqp://": RabbitMessaging,
    "amqps://": RabbitMessaging,
    "redis://": RedisMessaging,
    "rediss://": RedisMessaging,
    # "kafka://": KafkaMessaging,
    # "nats://": NatsMessaging,
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
