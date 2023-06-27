from dataclasses import dataclass, field
from uuid import uuid4

from repid._utils import FROZEN_DATACLASS, SLOTS_DATACLASS, VALID_ID, VALID_NAME
from repid.data.priorities import PrioritiesT


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
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

        if self.priority < 0:
            raise ValueError("Invalid priority.")
