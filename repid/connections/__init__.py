from repid.connections.abc import BucketBrokerT, ConsumerT, MessageBrokerT
from repid.connections.in_memory import (
    DummyBucketBroker,
    DummyMessageBroker,
    InMemoryBucketBroker,
    InMemoryMessageBroker,
)

__all__ = [
    "BucketBrokerT",
    "ConsumerT",
    "MessageBrokerT",
    "DummyBucketBroker",
    "DummyMessageBroker",
    "InMemoryBucketBroker",
    "InMemoryMessageBroker",
]

try:
    import aiormq
except ImportError:  # pragma: no cover
    pass
else:
    from repid.connections.rabbitmq import RabbitMessageBroker

    __all__.append("RabbitMessageBroker")

try:
    import redis
except ImportError:  # pragma: no cover
    pass
else:
    from repid.connections.redis import RedisBucketBroker, RedisMessageBroker

    __all__.append("RedisBucketBroker")
    __all__.append("RedisMessageBroker")
