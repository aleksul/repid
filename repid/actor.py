from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from repid.converter import ConverterT
    from repid.retry_policy import RetryPolicyT


class ActorData(NamedTuple):
    fn: Callable[..., Coroutine]
    name: str
    queue: str
    retry_policy: RetryPolicyT
    converter: ConverterT


class ActorResult(NamedTuple):
    data: str | None
    success: bool
    exception: Exception | None
    started_when: int
    finished_when: int
    reporting_done: bool
