from dataclasses import dataclass, field
from uuid import uuid4

from repid.data._const import SLOTS_DATACLASS
from repid.data.priorities import PrioritiesT
from repid.utils import VALID_ID, VALID_NAME


@dataclass(frozen=True, **SLOTS_DATACLASS)
class RoutingKey:
    topic: str
    queue: str = "default"
    priority: int = PrioritiesT.MEDIUM.value
    id_: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        if not VALID_ID.fullmatch(self.id_):
            raise ValueError("Incorrect id.")

        if not VALID_NAME.fullmatch(self.topic):
            raise ValueError("Incorrect topic.")

        if not VALID_NAME.fullmatch(self.queue):
            raise ValueError("Incorrect queue name.")
