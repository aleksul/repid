from dataclasses import dataclass
from typing import TYPE_CHECKING

from repid.utils import FROZEN_DATACLASS, SLOTS_DATACLASS

if TYPE_CHECKING:
    from repid.data.protocols import ParametersT, RoutingKeyT


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class Message:
    key: "RoutingKeyT"
    payload: str
    parameters: "ParametersT"
