import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Set, Union

if TYPE_CHECKING:
    from datetime import datetime

    from repid.data.protocols import MessageT, ParametersT


@dataclass
class DummyQueue:
    simple: asyncio.Queue = field(default_factory=asyncio.Queue)
    delayed: Dict["datetime", List["MessageT"]] = field(default_factory=dict)
    dead: List["MessageT"] = field(default_factory=list)
    processing: Set["MessageT"] = field(default_factory=set)


def wait_until(params: Union["ParametersT", None] = None) -> Union["datetime", None]:
    if params is None or params.delay is None:
        return None
    return params.delay.next_execution_time or params.compute_next_execution_time
