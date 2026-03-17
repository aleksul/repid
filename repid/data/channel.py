from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.asyncapi.models import ChannelBindingsObject

    from .external_docs import ExternalDocs


@dataclass(frozen=True, kw_only=True, slots=True)
class Channel:  # noqa: PLW1641
    address: str
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    bindings: ChannelBindingsObject | None = None
    external_docs: ExternalDocs | None = None

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Channel):
            return self.address == value.address
        raise ValueError(f"Cannot compare Channel with {type(value)}.")
