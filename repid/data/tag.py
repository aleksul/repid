from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .external_docs import ExternalDocs


@dataclass(frozen=True, slots=True, kw_only=True)
class Tag:
    name: str
    description: str | None = None
    external_docs: ExternalDocs | None = None
