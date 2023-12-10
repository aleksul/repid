from __future__ import annotations

import random
import re
import time
from typing import TYPE_CHECKING

from repid.data import PrioritiesT

if TYPE_CHECKING:
    from repid.data import ParametersT, RoutingKeyT

VALID_PRIORITIES = re.compile(r"[0-9]+\/[0-9]+\/[0-9]+")


def get_priorities_order(priorities_chanses: list[float]) -> list[PrioritiesT]:
    """Randomizes the order in which priorities should be processed,
    based on the supplied chances.

    Args:
        priorities_chanses (List[float]): list of floats, representing chances, e.g.
        `[0.6, 0.3, 0.1]`.

    Returns:
        List[PrioritiesT]: 3 priorities in order of processing.
    """
    rand = random.random()  # noqa: S311
    if rand <= priorities_chanses[0]:
        return [PrioritiesT.HIGH, PrioritiesT.MEDIUM, PrioritiesT.LOW]
    if rand <= priorities_chanses[0] + priorities_chanses[1]:
        return [PrioritiesT.MEDIUM, PrioritiesT.HIGH, PrioritiesT.LOW]
    return [PrioritiesT.LOW, PrioritiesT.HIGH, PrioritiesT.MEDIUM]


def parse_priorities_distribution(priorities_distribution: str) -> list[float]:
    """Turns priorities distribution string into list of floats.

    Args:
        priorities_distribution (str): distribution of priorities, from highest to lowest, e.g.
        `10/3/1`.

    Raises:
        ValueError: if priorities_distribution does not match `VALID_PRIORITIES` regex.

    Returns:
        List[float]: list of 3 floats, representing chances, e.g. `[0.6, 0.3, 0.1]`.
    """
    if not VALID_PRIORITIES.fullmatch(priorities_distribution):  # pragma: no cover
        raise ValueError(f"Invalid priorities distribution: {priorities_distribution}")
    pr_dist = [int(x) for x in priorities_distribution.split("/")]
    pr_dist_sum = sum(pr_dist)
    return [x / pr_dist_sum for x in pr_dist]


def qnc(
    queue_name: str,
    priority: int = PrioritiesT.MEDIUM.value,
    *,
    delayed: bool = False,
    dead: bool = False,
) -> str:
    """Queue name constructor.

    Args:
        queue_name (str): name of the queue
        priority (int, optional): priority of the queue. Defaults to PrioritiesT.MEDIUM.
        delayed (bool, optional): True, if the queue is for delayed messages. Defaults to False.
        dead (bool, optional): True, if the queue is dead-letter.
        If set to True, takes precedence over `delayed`. Defaults to False.

    Returns:
        str: queue name, representing all the arguments.
    """
    if dead:
        return f"q:{queue_name}:{priority}:dead"
    return f"q:{queue_name}:{priority}:{'d' if delayed else 'n'}"


def mnc(
    key: RoutingKeyT,
    *,
    short: bool = False,
) -> str:
    """Message name constructor.

    Args:
        key (RoutingKeyT): routing key to construct name for.
        short (bool, optional): If True, omits `m:{key.queue}:{key.priority}:` prefix.
        Defaults to False.

    Returns:
        str: message name, representing all the arguments.
    """
    prefix = ""
    if not short:
        prefix = f"m:{key.queue}:{key.priority}:"
    return f"{prefix}{key.topic}:{key.id_}"


def full_message_name_from_short(short_name: str, full_queue_name: str) -> str:
    _, queue_name, priority, _ = full_queue_name.split(":")
    return f"m:{queue_name}:{priority}:{short_name}"


def get_queue_marker(full_queue_name: str) -> str:
    return full_queue_name.split(":")[-1]


def parse_short_message_name(short_name: str) -> tuple[str, str]:
    topic, id_ = short_name.split(":")
    return (topic, id_)


def parse_message_name(name: str) -> tuple[str, str, str, int]:
    _, queue, priority, topic, id_ = name.split(":")
    return (id_, topic, queue, int(priority))


def unix_time() -> int:
    return int(time.time())


def wait_timestamp(params: ParametersT | None = None) -> int | None:
    if params is None or params.delay is None:
        return None

    if params.delay.next_execution_time is not None:
        return int(params.delay.next_execution_time.timestamp())

    if (computed := params.compute_next_execution_time) is not None:
        return int(computed.timestamp())

    return None
