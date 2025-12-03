from __future__ import annotations

import inspect
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Protocol, get_origin, runtime_checkable

from repid.dependencies.header_dependency import Header
from repid.dependencies.message_dependency import MessageDependency

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT, ServerT
    from repid.data.actor import ActorData
    from repid.serializer import SerializerT


@dataclass(slots=True, kw_only=True, frozen=True)
class DependencyContext:
    message: ReceivedMessageT
    actor: ActorData
    server: ServerT
    default_serializer: SerializerT
    parsed_args: list[Any]
    parsed_kwargs: dict[str, Any]
    parsed_headers: dict[str, Any]
    headers_id_to_name: dict[int, str]


@runtime_checkable
class DependencyT(Protocol):
    async def resolve(self, *, context: DependencyContext) -> Any: ...


def get_dependency(t: Any) -> DependencyT | None:
    """Extract a dependency instance from a type annotation if present."""
    if get_origin(t) is not Annotated:
        return None
    for metadata in t.__metadata__:
        # Skip classes - they need to be instantiated to work as dependencies
        if inspect.isclass(metadata) and issubclass(metadata, (Header, MessageDependency)):
            continue
        if isinstance(metadata, DependencyT):
            return metadata
    return None


def validate_dependency(t: Any) -> None:
    """Validate a type annotation and emit warnings for common misuses."""
    if get_origin(t) is not Annotated:
        return
    for metadata in t.__metadata__:
        if inspect.isclass(metadata) and issubclass(metadata, (Header, MessageDependency)):
            warnings.warn(
                "Using Header or Message classes directly as dependency has no effect.",
                UserWarning,
                stacklevel=2,
            )
            break
