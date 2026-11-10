from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from repid.connections.abc import ReceivedMessageT, ServerT

logger = logging.getLogger("repid")


@dataclass(slots=True)
class _RenewalTiming:
    """Carry the next lease renewal across lifecycle phases."""

    next_renewal: float | None = None


async def _keep_alive_loop(
    message: ReceivedMessageT,
    interval: float,
    timing: _RenewalTiming | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    delay = interval
    if timing is not None and timing.next_renewal is not None:
        delay = max(0, min(interval, timing.next_renewal - loop.time()))
    while True:
        if timing is not None:
            timing.next_renewal = loop.time() + delay
        await asyncio.sleep(delay)
        try:
            if await message.keep_alive() is False:
                return
        except Exception:  # noqa: BLE001
            logger.warning("message.keep_alive.error", extra={"message_id": message.message_id})
        delay = interval


@asynccontextmanager
async def _keep_message_alive(
    message: ReceivedMessageT,
    interval: float | None,
    server: ServerT,
    timing: _RenewalTiming | None = None,
) -> AsyncGenerator[None]:
    if interval is None or not server.capabilities["supports_keep_alive"]:
        yield
        return

    coroutine = _keep_alive_loop(message, interval, timing)
    try:
        task = asyncio.create_task(coroutine)
    except BaseException:
        coroutine.close()
        raise
    try:
        yield
    finally:
        task.cancel()
        # Consume the keep-alive task's cancellation while propagating cancellation of this scope.
        await asyncio.gather(task, return_exceptions=True)
