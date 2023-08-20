from repid._utils import is_installed
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

if is_installed("aiormq"):
    from repid.connections.rabbitmq import RabbitMessageBroker

    __all__ += ["RabbitMessageBroker"]

if is_installed("redis"):
    from repid.connections.redis import RedisBucketBroker, RedisMessageBroker

    __all__ += ["RedisBucketBroker", "RedisMessageBroker"]
