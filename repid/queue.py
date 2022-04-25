from typing import TYPE_CHECKING, Optional

from repid import _default_connection
from repid.utils import VALID_NAME

if TYPE_CHECKING:
    from repid.connections.connection import Connection


class Queue:
    __slots__ = ("name", "__conn")

    def __init__(
        self,
        name: str,
        _connection: Optional[Connection] = None,
    ) -> None:
        self.name = name
        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Queue name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )

        self.__conn: Connection = _connection or _default_connection  # type: ignore[assignment]
        if self.__conn is None:
            raise ValueError("No connection provided.")

    async def declare(self):
        await self.__conn.messager.queue_declare(self.name)

    async def flush(self):
        await self.__conn.messager.queue_flush(self.name)

    async def delete(self):
        await self.__conn.messager.queue_delete(self.name)

    def __str__(self) -> str:
        return f"Queue({self.name})"
