from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from repid.limits import IntakeControl, LimitPolicyT, MessageLimits

if TYPE_CHECKING:
    from repid.asyncapi.models import ChannelBindingsObject

    from .external_docs import ExternalDocs


@dataclass(frozen=True, kw_only=True, slots=True)
class Channel:  # noqa: PLW1641
    address: str
    limits: MessageLimits | None = None
    limit_policies: tuple[LimitPolicyT, ...] = ()
    intake_control: IntakeControl | None = None
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    bindings: ChannelBindingsObject | None = None
    external_docs: ExternalDocs | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "limit_policies", tuple(self.limit_policies))

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Channel):
            return self.address == value.address
        raise ValueError(f"Cannot compare Channel with {type(value)}.")
