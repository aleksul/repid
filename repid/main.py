from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, AsyncGenerator

if TYPE_CHECKING:
    from repid.connection import Connection

DEFAULT_CONNECTION: ContextVar[Connection] = ContextVar("DEFAULT_CONNECTION")


class Repid:
    def __init__(self, connection: Connection):
        self._conn = connection
        self.add_middleware = self._conn.middleware.add_middleware

    @asynccontextmanager
    async def connect(self, disconnect: bool = True) -> AsyncGenerator[Connection, None]:
        contextvar_token = DEFAULT_CONNECTION.set(self._conn)
        await self._conn.connect()
        try:
            yield self._conn
        finally:
            DEFAULT_CONNECTION.reset(contextvar_token)
            if disconnect:
                await self._conn.disconnect()
