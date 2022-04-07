from dataclasses import dataclass

from .utils import VALID_NAME_RE


@dataclass(frozen=True)
class Queue:
    name: str

    def __post_init__(self) -> None:
        if not VALID_NAME_RE.fullmatch(self.name):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
