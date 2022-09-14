from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.connection import Connection

DEFAULT_CONNECTION: ContextVar[Connection] = ContextVar("DEFAULT_CONNECTION")


class Repid:
    def __init__(self, connection: Connection):
        self._conn = connection
        self.add_middleware = self._conn.middleware.add_middleware

    async def __aenter__(self) -> None:
        DEFAULT_CONNECTION.set(self._conn)
