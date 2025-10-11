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
