from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.dependencies._utils import DependencyContext


class MessageDependency:
    """Dependency annotation that indicates that the argument resolves to the received message."""

    async def resolve(self, *, context: DependencyContext) -> ReceivedMessageT:
        return context.message


# Type alias for convenience
Message = Annotated["ReceivedMessageT", MessageDependency()]
