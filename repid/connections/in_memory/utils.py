from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from repid.data.protocols import ParametersT, RoutingKeyT


@dataclass(frozen=True)
class Message:
    key: RoutingKeyT
    payload: str
    parameters: ParametersT


@dataclass
class DummyQueue:
    simple: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    delayed: dict[datetime, list[Message]] = field(default_factory=dict)
    dead: list[Message] = field(default_factory=list)
    processing: set[Message] = field(default_factory=set)


def wait_until(params: ParametersT | None = None) -> datetime | None:
    if params is None or params.delay is None:
        return None
    return params.delay.next_execution_time or params.compute_next_execution_time
