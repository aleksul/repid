from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator, Iterable

from repid._utils import VALID_NAME
from repid.main import Repid
from repid.message import Message, MessageCategory

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
                "followed by letters, digits, dashes or underscores.",
            )

    async def get_messages(
        self,
        topics: Iterable[str] | None = None,
        max_unacked_messages: int | None = None,
        category: MessageCategory = MessageCategory.NORMAL,
    ) -> AsyncIterator[Message]:
        consumer = self._conn.message_broker.get_consumer(
            queue_name=self.name,
            topics=topics,
            max_unacked_messages=max_unacked_messages,
            category=category,
        )
        async with consumer:
            async for key, raw_payload, parameters in consumer:
                yield Message(
                    key=key,
                    raw_payload=raw_payload,
                    parameters=parameters,
                    _connection=self._conn,
                    _category=category,
                )

    async def declare(self) -> None:
        await self._conn.message_broker.queue_declare(self.name)

    async def flush(self) -> None:
        await self._conn.message_broker.queue_flush(self.name)

    async def delete(self) -> None:
        await self._conn.message_broker.queue_delete(self.name)

    def __str__(self) -> str:
        return f"Queue({self.name})"
