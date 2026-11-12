from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True, slots=True)
class Concurrency:
    """Message count and serialized payload byte budgets.

    Each field accepts a positive integer or None for unlimited. A delivery
    larger than a byte budget waits for that scope to empty, then runs alone
    within it. Worker and channel budgets both apply when configured.
    """

    messages: int | None = None
    payload_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.messages is not None and (
            isinstance(self.messages, bool)
            or not isinstance(self.messages, int)
            or self.messages <= 0
        ):
            raise ValueError("messages must be a positive integer or None")
        if self.payload_bytes is not None and (
            isinstance(self.payload_bytes, bool)
            or not isinstance(self.payload_bytes, int)
            or self.payload_bytes <= 0
        ):
            raise ValueError("payload_bytes must be a positive integer or None")
