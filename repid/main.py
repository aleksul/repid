from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator

from repid.config import Config
from repid.connection import Connection


class Repid:
    __local = threading.local()

    def __init__(
        self,
        connection: Connection,
        *,
        middlewares: list | None = None,
        update_config: bool = False,
    ):
        self.connection = connection

        if middlewares is not None:
            for middleware in middlewares:
                self.connection.middleware.add_middleware(middleware)

        if update_config:
            Config.update_all()
            # In brokers Config is only fetched on initialization, and by the time this is called -
            # initialization has been already done, so we have to update manually
            self.connection._update_from_config()

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
        if hasattr(Repid.__local, "connection"):
            delattr(Repid.__local, "connection")
        return self.connection

    @asynccontextmanager
    async def magic(self, *, auto_disconnect: bool = False) -> AsyncIterator[Connection]:
        await self.magic_connect()
        try:
            yield self.connection
        finally:
            if auto_disconnect:
                await self.magic_disconnect()
