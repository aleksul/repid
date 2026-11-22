from __future__ import annotations

import asyncio
import inspect
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Protocol, get_origin, runtime_checkable

from repid.dependencies.full_payload import FullPayload
from repid.dependencies.header_dependency import Header
from repid.dependencies.message_dependency import MessageDependency

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData, ActorExecutionContext


@dataclass(slots=True, kw_only=True, frozen=True)
class DependencyContext:
    message: ReceivedMessageT
    actor: ActorData
    actor_context: ActorExecutionContext
    parsed_args: list[Any]
    parsed_kwargs: dict[str, Any]
    parsed_headers: dict[str, Any]
    headers_id_to_name: dict[int, str]


@runtime_checkable
class DependencyT(Protocol):
    async def resolve(self, *, context: DependencyContext) -> Any: ...


# Lookups that never suspend. Running them inline avoids a task per dependency per message.
_INLINE_DEPENDENCIES = (Header, MessageDependency)


async def resolve_dependencies(
    dependencies: Mapping[str, DependencyT],
    context: DependencyContext,
) -> dict[str, Any]:
    """Resolve dependencies by name, running the remaining ones concurrently as tasks."""
    resolved: dict[str, Any] = {}
    concurrent: list[tuple[str, DependencyT]] = []
    for name, dependency in dependencies.items():
        if isinstance(dependency, _INLINE_DEPENDENCIES):
            resolved[name] = await dependency.resolve(context=context)
        else:
            concurrent.append((name, dependency))
    if concurrent:
        # Create tasks only after inline lookups succeed, so a failed lookup
        # starts no dependency.
        tasks = [
            asyncio.create_task(dependency.resolve(context=context)) for _, dependency in concurrent
        ]
        try:
            values = await asyncio.gather(*tasks)
        except BaseException:
            await _cancel_remaining(tasks)
            raise
        for (name, _), value in zip(concurrent, values, strict=True):
            resolved[name] = value
    return resolved


async def _cancel_remaining(tasks: list[asyncio.Task[Any]]) -> None:
    """Stop dependencies still running after one failed or the caller was cancelled."""
    for task in tasks:
        task.cancel()
    await asyncio.wait(tasks)
    for task in tasks:
        if not task.cancelled():
            # Their failures are superseded by the one being raised.
            task.exception()


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


def get_full_payload_marker(t: Any) -> FullPayload | None:
    """Return the `FullPayload` instance from a type annotation if present."""
    if get_origin(t) is not Annotated:
        return None
    for metadata in t.__metadata__:
        if isinstance(metadata, FullPayload):
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
