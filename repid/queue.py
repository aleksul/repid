from __future__ import annotations

from typing import TYPE_CHECKING

from repid.main import Repid
from repid.utils import VALID_NAME

if TYPE_CHECKING:
    from repid.connection import Connection


class Queue:
    __slots__ = ("name", "_conn")

    def __init__(
        self,
        name: str = "default",
        _connection: Connection | None = None,
    ) -> None:
        self._conn = _connection or Repid.get_magic_connection()

        self.name = name
        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Queue name must start with a letter or an underscore "
                "followed by letters, digits, dashes or underscores."
            )

    async def declare(self) -> None:
        await self._conn.message_broker.queue_declare(self.name)

    async def flush(self) -> None:
        await self._conn.message_broker.queue_flush(self.name)

    async def delete(self) -> None:
        await self._conn.message_broker.queue_delete(self.name)

    def __str__(self) -> str:
        return f"Queue({self.name})"
