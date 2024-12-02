from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Coroutine, NamedTuple

if TYPE_CHECKING:
    from repid.converter import ConverterT
    from repid.retry_policy import RetryPolicyT


class ActorData(NamedTuple):
    """ActorData tuple is used to hold data related to an actor."""

    fn: Callable[..., Coroutine]
    name: str
    queue: str
    retry_policy: RetryPolicyT
    converter: ConverterT


class ActorResult(NamedTuple):
    """ActorResult tuple is used to hold metadata of an actor's execution."""

    data: str | None
    success: bool
    exception: Exception | None
    started_when: int
    finished_when: int
    reporting_done: bool
