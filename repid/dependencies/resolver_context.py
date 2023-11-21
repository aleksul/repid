from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from repid._utils import FROZEN_DATACLASS, SLOTS_DATACLASS

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.connection import Connection
    from repid.data.protocols import ParametersT, RoutingKeyT


@dataclass(**SLOTS_DATACLASS, **FROZEN_DATACLASS)
class ResolverContext:
    message_key: RoutingKeyT
    message_raw_payload: str
    message_parameters: ParametersT
    connection: Connection
    actor_data: ActorData
    actor_processing_started_when: int
