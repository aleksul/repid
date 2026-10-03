"""Cancellation-safe numeric reservations; delivery accounting lives in the runner."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from repid.limits import MessageLimits


@dataclass(eq=False)
class Pool:
    limits: MessageLimits
    messages: int = 0
    payload_bytes: int = 0
    waiters: list[tuple[object, int]] = field(default_factory=list)

    def ready(self, token: object, size: int) -> bool:
        count = self.limits.max_messages
        byte_cap = self.limits.max_payload_bytes
        for waiting, price in self.waiters:
            if waiting is token:
                break
            if byte_cap is not None and price > byte_cap:
                return False
        if count is not None and self.messages >= count:
            return False
        if byte_cap is None:
            return True
        if size > byte_cap:
            return self.messages == 0
        return self.payload_bytes + size <= byte_cap


class Capacity:
    def __init__(self) -> None:
        self.condition = asyncio.Condition()

    async def acquire(
        self,
        pools: tuple[Pool, ...],
        size: int,
        on_wait: Callable[[], Awaitable[None]],
    ) -> None:
        token = object()
        for pool in pools:
            pool.waiters.append((token, size))
        try:
            if not all(pool.ready(token, size) for pool in pools):
                await on_wait()
            async with self.condition:
                await self.condition.wait_for(
                    lambda: all(pool.ready(token, size) for pool in pools),
                )
                for pool in pools:
                    pool.messages += 1
                    pool.payload_bytes += size
        finally:
            async with self.condition:
                for pool in pools:
                    pool.waiters[:] = [
                        (waiter, price) for waiter, price in pool.waiters if waiter is not token
                    ]
                self.condition.notify_all()

    async def release(self, pools: tuple[Pool, ...], size: int) -> None:
        async with self.condition:
            for pool in pools:
                pool.messages -= 1
                pool.payload_bytes -= size
            self.condition.notify_all()

    @asynccontextmanager
    async def reserve(
        self,
        pool: Pool,
        size: int,
        on_wait: Callable[[], Awaitable[None]],
    ) -> AsyncIterator[None]:
        await self.acquire((pool,), size, on_wait)
        try:
            yield
        finally:
            await self.release((pool,), size)
