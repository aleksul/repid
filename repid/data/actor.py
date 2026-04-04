from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeVar

if TYPE_CHECKING:
    from repid.asyncapi.models.operations import OperationBindingsObject
    from repid.connections.abc import BaseMessageT, ReceivedMessageT
    from repid.converter import ConverterT
    from repid.data import ExternalDocs, Tag
    from repid.data.message_schema import ActorMessageMetadata

FnReturnT = TypeVar("FnReturnT")


AutoActionT = Literal["ack", "nack", "reject"]
OnErrorAutoT = AutoActionT | Callable[[BaseException], AutoActionT]

ManualActionT = Literal["ack", "nack", "reject", "no_action"]
OnErrorManualT = ManualActionT | Callable[[BaseException], ManualActionT]

OnErrorT = OnErrorAutoT | OnErrorManualT


@dataclass(frozen=True, kw_only=True, slots=True)
class ActorData:
    fn: Callable[..., Coroutine[Any, Any, FnReturnT]]
    name: str
    confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual", "manual_explicit"]
    routing_strategy: Callable[[BaseMessageT], bool]
    middleware_pipeline: Callable[
        [
            Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, FnReturnT]],
            ReceivedMessageT,
            ActorData,
        ],
        Coroutine,
    ]
    channel_address: str
    converter: ConverterT
    on_error: OnErrorT = "nack"
    timeout: float = 300.0
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    security: tuple[Any, ...] | None = None
    tags: tuple[Tag, ...] | None = None
    external_docs: ExternalDocs | None = None
    bindings: OperationBindingsObject | None = None
    deprecated: bool = False
    message_schema: ActorMessageMetadata | None = None
