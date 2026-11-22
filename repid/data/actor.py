from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

if TYPE_CHECKING:
    from repid.asyncapi.models.operations import OperationBindingsObject
    from repid.connections.abc import BaseMessageT, MessagePublisherT, ReceivedMessageT, ServerT
    from repid.converter import ConverterT
    from repid.data import ExternalDocs, Tag
    from repid.data.message_schema import ActorMessageMetadata
    from repid.serializer import SerializerT

# An actor function as written: `async def`, or a plain `def` that Repid runs in
# an executor. It may return anything.
ActorFunctionT: TypeAlias = Callable[..., Coroutine[Any, Any, Any]] | Callable[..., Any]

# The next step of a middleware pipeline. Actor results are arbitrary user data.
_ActorCall: TypeAlias = Callable[["ReceivedMessageT", "ActorData"], Coroutine[Any, Any, Any]]


AutoActionT = Literal["ack", "nack", "reject"]
OnErrorAutoT = AutoActionT | Callable[[BaseException], AutoActionT]

ManualActionT = Literal["ack", "nack", "reject", "no_action"]
OnErrorManualT = ManualActionT | Callable[[BaseException], ManualActionT]

OnErrorT = OnErrorAutoT | OnErrorManualT


@dataclass(frozen=True, kw_only=True, slots=True)
class ActorExecutionContext:
    server: ServerT
    publish: MessagePublisherT
    default_serializer: SerializerT


@dataclass(frozen=True, kw_only=True, slots=True)
class ActorData:
    # Already wrapped into a coroutine function.
    fn: Callable[..., Coroutine[Any, Any, Any]]
    name: str
    confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual", "manual_explicit"]
    routing_strategy: Callable[[BaseMessageT], bool]
    middleware_pipeline: Callable[
        [_ActorCall, ReceivedMessageT, ActorData],
        Coroutine[Any, Any, Any],
    ]
    channel_address: str
    converter: ConverterT
    on_error: OnErrorT = "nack"
    timeout: float = 300.0
    keep_alive: bool | float | None = True
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    security: tuple[Any, ...] | None = None
    tags: tuple[Tag, ...] | None = None
    external_docs: ExternalDocs | None = None
    bindings: OperationBindingsObject | None = None
    deprecated: bool = False
    message_schema: ActorMessageMetadata | None = None
