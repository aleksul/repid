from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repid.data import CorrelationId, MessageExample


@dataclass(frozen=True, kw_only=True, slots=True)
class ConverterInputSchema:
    payload_schema: Any
    content_type: str
    headers_schema: Any | None = None
    correlation_id: CorrelationId | None = None
    examples: tuple[MessageExample, ...] | None = None
