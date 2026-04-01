from repid._utils import is_installed
from repid.connections.abc import (
    BaseMessageT,
    CapabilitiesT,
    MessageAction,
    ReceivedMessageT,
    SentMessageT,
    ServerT,
    SubscriberT,
)
from repid.connections.amqp import AmqpServer
from repid.connections.in_memory import InMemoryServer

__all__ = [
    "AmqpServer",
    "BaseMessageT",
    "CapabilitiesT",
    "InMemoryServer",
    "MessageAction",
    "ReceivedMessageT",
    "SentMessageT",
    "ServerT",
    "SubscriberT",
]


if is_installed("redis"):
    from repid.connections.redis import RedisServer

    __all__ += ["RedisServer"]

# check via try-import because pubsub is a sub-package of gcloud.aio
imported_pubsub = False
try:
    import google.auth
    import grpc.aio
except ImportError:  # pragma: no cover
    imported_pubsub = False
else:
    imported_pubsub = True

if imported_pubsub:
    from repid.connections.pubsub import PubsubServer

    __all__ += ["PubsubServer"]
