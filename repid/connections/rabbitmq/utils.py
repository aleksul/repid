from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from datetime import datetime

    from repid.data.protocols import ParametersT, RoutingKeyT


def durable_message_decider(key: RoutingKeyT) -> bool:
    """Decides if queue is set as durable in RabbitMQ."""
    return True


def qnc(queue_name: str, delayed: bool = False, dead: bool = False) -> str:
    """Queue name constructor for RabbitMQ."""
    if dead:
        return f"{queue_name}:dead"
    if delayed:
        return f"{queue_name}:delayed"
    return queue_name


def wait_until(params: ParametersT | None = None) -> datetime | None:
    if params is None or params.delay is None:
        return None
    return params.delay.next_execution_time or params.compute_next_execution_time


class MessageContent(TypedDict):
    payload: str
    parameters: str
