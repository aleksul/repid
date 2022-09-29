from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from repid.data.protocols import RoutingKeyT


class QueueNameConstructorT(Protocol):
    def __call__(self, queue_name: str, delayed: bool = False, dead: bool = False) -> str:
        ...


class DurableMessageDeciderT(Protocol):
    def __call__(self, key: RoutingKeyT) -> bool:
        ...
