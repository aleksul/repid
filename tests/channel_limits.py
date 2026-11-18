from __future__ import annotations

from collections.abc import Iterable, Mapping

from repid._concurrency import _DEFAULT_WORKER_CONCURRENCY, _Budget
from repid.data.concurrency import Concurrency


def channel_limits(
    channels: Iterable[str],
    worker: Concurrency | None = None,
    configured: Mapping[str, Concurrency] | None = None,
) -> dict[str, Concurrency]:
    """Build the effective per-channel limits a worker passes to `ServerT.subscribe`."""
    budget = _Budget(worker or _DEFAULT_WORKER_CONCURRENCY, configured)
    return {channel: budget.limits(channel) for channel in channels}
