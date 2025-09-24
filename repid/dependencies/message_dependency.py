from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.dependencies.resolver_context import ResolverContext


class MessageDependency:
    async def resolve(self, *, context: ResolverContext) -> ReceivedMessageT:
        return context.message


MessageDependencyT = Annotated["ReceivedMessageT", MessageDependency()]
