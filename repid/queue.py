from typing import Union

from repid.connection import Connection
from repid.main import DEFAULT_CONNECTION
from repid.utils import VALID_NAME


class Queue:
    __slots__ = ("name", "__conn")

    def __init__(
        self,
        name: str = "default",
        _connection: Union[Connection, None] = None,
    ) -> None:
        self.name = name
        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Queue name must start with a letter or an underscore "
                "followed by letters, digits, dashes or underscores."
            )

        self.__conn = _connection or DEFAULT_CONNECTION.get()

    async def declare(self) -> None:
        await self.__conn.messager.queue_declare(self.name)

    async def flush(self) -> None:
        await self.__conn.messager.queue_flush(self.name)

    async def delete(self) -> None:
        await self.__conn.messager.queue_delete(self.name)

    def __str__(self) -> str:
        return f"Queue({self.name})"
