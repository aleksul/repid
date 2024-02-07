from __future__ import annotations

from collections import UserDict
from typing import TYPE_CHECKING, Any, TypedDict, cast

if TYPE_CHECKING:
    from datetime import datetime

    from repid.connections.rabbitmq.consumer import _RabbitConsumer
    from repid.connections.rabbitmq.protocols import RabbitConsumerCallbackT
    from repid.data.protocols import ParametersT, RoutingKeyT

    # Python 3.8 typing trick
    TypedUserDict = UserDict[str, RabbitConsumerCallbackT]
else:
    TypedUserDict = UserDict


def durable_message_decider(key: RoutingKeyT) -> bool:  # noqa: ARG001
    """Decides if queue is set as durable in RabbitMQ."""
    return True


def qnc(queue_name: str, *, delayed: bool = False, dead: bool = False) -> str:
    """Queue name constructor for RabbitMQ."""
    if dead:  # pragma: no cover
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


class _Consumers(TypedUserDict):
    """A special dictionary which informs Repid's RabbitMQ consumer
    that aiormq has removed consumer's callback from the channel."""

    __marker = object()

    def pop(self, key: str, default: Any = __marker) -> Any:
        try:
            value = self[key]
        except KeyError:  # pragma: no cover
            if default is self.__marker:
                raise
            return default
        else:
            cast("_RabbitConsumer", value.__self__).server_side_cancel_event.set()
            del self[key]
            return value
