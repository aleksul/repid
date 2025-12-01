from repid._utils import is_installed
from repid.connections.abc import ServerT
from repid.connections.in_memory import InMemoryServer

__all__ = [
    "InMemoryServer",
    "ServerT",
]


if is_installed("aiormq"):
    from repid.connections.amqp import AmqpServer

    __all__ += ["AmqpServer"]

if is_installed("redis"):
    from repid.connections.redis import RedisServer

    __all__ += ["RedisServer"]

# check via try-import because pubsub is a sub-package of gcloud.aio
imported_pubsub = False
try:
    import google.auth
    import grpc.aio
except ImportError:
    imported_pubsub = False
else:
    imported_pubsub = True

if imported_pubsub:
    from repid.connections.pubsub import PubsubServer

    __all__ += ["PubsubServer"]
