from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Protocol, get_origin, runtime_checkable

from repid.dependencies.header_dependency import Header
from repid.dependencies.message_dependency import MessageDependency

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData


@dataclass(slots=True, kw_only=True, frozen=True)
class DependencyContext:
    message: ReceivedMessageT
    actor: ActorData
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
        if isinstance(metadata, DependencyT):
            return metadata
        if issubclass(metadata, (Header, MessageDependency)):
            warnings.warn(
                "Using Header or Message classes directly as dependency has no effect.",
                UserWarning,
                stacklevel=2,
            )
            break
    return None
