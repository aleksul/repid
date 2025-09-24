from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData


@dataclass(slots=True, kw_only=True, frozen=True)
class ResolverContext:
    message: ReceivedMessageT
    actor: ActorData
