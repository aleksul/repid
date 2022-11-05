from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator

from repid.connection import Connection


class Repid:
    __local = threading.local()

    def __init__(self, connection: Connection):
        self.connection = connection
        self.add_middleware = self.connection.middleware.add_middleware

    @classmethod
    def get_magic_connection(cls) -> Connection:
        if hasattr(cls.__local, "connection") and isinstance(cls.__local.connection, Connection):
            return cls.__local.connection
        raise ValueError("Default connection isn't set.")

    async def magic_connect(self) -> Connection:
        if not self.connection.is_open:
            await self.connection.connect()
        Repid.__local.connection = self.connection
        return self.connection

    async def magic_disconnect(self) -> Connection:
        await self.connection.disconnect()
        delattr(Repid.__local, "connection")
        return self.connection

    @asynccontextmanager
    async def magic(self, auto_disconnect: bool = False) -> AsyncIterator[Connection]:
        await self.magic_connect()
        try:
            yield self.connection
        finally:
            if auto_disconnect:
                await self.magic_disconnect()
