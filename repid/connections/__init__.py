from repid.connections.abc import BucketBrokerT, ConsumerT, MessageBrokerT
from repid.connections.in_memory import (
    DummyBucketBroker,
    DummyMessageBroker,
    InMemoryBucketBroker,
    InMemoryMessageBroker,
)
from repid.utils import is_installed

__all__ = [
    "BucketBrokerT",
    "ConsumerT",
    "MessageBrokerT",
    "DummyBucketBroker",
    "DummyMessageBroker",
    "InMemoryBucketBroker",
    "InMemoryMessageBroker",
]

if is_installed("aiormq"):
    from repid.connections.rabbitmq import RabbitMessageBroker

    __all__.append("RabbitMessageBroker")

if is_installed("redis"):
    from repid.connections.redis import RedisBucketBroker, RedisMessageBroker

    __all__.append("RedisBucketBroker")
    __all__.append("RedisMessageBroker")
