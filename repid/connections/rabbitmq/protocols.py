from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import asyncio

    import aiormq

    from repid.data.protocols import RoutingKeyT


class QueueNameConstructorT(Protocol):
    def __call__(self, queue_name: str, *, delayed: bool = False, dead: bool = False) -> str: ...


class DurableMessageDeciderT(Protocol):
    def __call__(self, key: RoutingKeyT) -> bool: ...


class RabbitConsumerT(Protocol):
    @property
    def server_side_cancel_event(self) -> asyncio.Event: ...

    async def on_new_message(self, message: aiormq.abc.DeliveredMessage) -> None: ...


class RabbitConsumerCallbackT(Protocol):
    async def __call__(self, message: aiormq.abc.DeliveredMessage) -> None: ...

    @property
    def __self__(self) -> RabbitConsumerT: ...
