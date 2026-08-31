from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from repid.limits import dedupe_by_identity

if TYPE_CHECKING:
    from repid.asyncapi.models import ChannelBindingsObject
    from repid.limits import LimitPolicyT, MessageLimits

    from .external_docs import ExternalDocs


@dataclass(frozen=True, kw_only=True, slots=True)
class Channel:  # noqa: PLW1641
    address: str
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    bindings: ChannelBindingsObject | None = None
    external_docs: ExternalDocs | None = None
    limits: MessageLimits | None = None
    limit_policies: Sequence[LimitPolicyT] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "limit_policies",
            tuple(dedupe_by_identity(self.limit_policies)),
        )

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Channel):
            return self.address == value.address
        raise ValueError(f"Cannot compare Channel with {type(value)}.")
