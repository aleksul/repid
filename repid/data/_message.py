from dataclasses import dataclass
from typing import TYPE_CHECKING

from repid.data._const import SLOTS_DATACLASS

if TYPE_CHECKING:
    from repid.data.protocols import ParametersT, RoutingKeyT


@dataclass(frozen=True, **SLOTS_DATACLASS)
class Message:
    key: RoutingKeyT
    payload: str
    parameters: ParametersT
