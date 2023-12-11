from __future__ import annotations

from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

from repid.main import Repid

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.data.protocols import ParametersT, RoutingKeyT


class MessageCategory(str, Enum):
    NORMAL = "NORMAL"
    DELAYED = "DELAYED"
    DEAD = "DEAD"


class Message:
    __slots__ = (
        "_key",
        "raw_payload",
        "parameters",
        "_connection",
        "_category",
        "__read_only",
    )

    def __init__(
        self,
        *,
        key: RoutingKeyT,
        raw_payload: str,
        parameters: ParametersT,
        _connection: Connection | None = None,
        _category: MessageCategory = MessageCategory.NORMAL,
    ) -> None:
        self._key = key
        self.raw_payload = raw_payload
        self.parameters = parameters
        self._connection = _connection or Repid.get_magic_connection()
        self._category = _category if _category is not None else MessageCategory.NORMAL
        self.__read_only = False

    @property
    def key(self) -> RoutingKeyT:
        return self._key

    @property
    def read_only(self) -> bool:
        return self.__read_only

    @property
    def category(self) -> MessageCategory:
        return self._category

    async def ack(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.ack(self._key)

        self.__read_only = True

    async def nack(self) -> None:
        if self._category != MessageCategory.NORMAL:
            raise ValueError(f"Can not nack message with category {self._category}.")

        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.nack(self._key)

        self.__read_only = True

    async def reject(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.reject(self._key)

        self.__read_only = True

    async def reschedule(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.requeue(
            self._key,
            self.raw_payload,
            self.parameters._prepare_reschedule(),
        )

        self.__read_only = True

    async def retry(self, next_retry: timedelta | None = None) -> None:
        if self._category != MessageCategory.NORMAL:
            raise ValueError(f"Can not retry message with category {self._category}.")

        if self.__read_only:
            raise ValueError("Message is read only.")

        if self.parameters.retries.already_tried >= self.parameters.retries.max_amount:
            raise ValueError("Max retry limit reached.")

        await self._connection.message_broker.requeue(
            self._key,
            self.raw_payload,
            self.parameters._prepare_retry(
                next_retry=timedelta(seconds=0) if next_retry is None else next_retry,
            ),
        )

        self.__read_only = True

    async def force_retry(self, next_retry: timedelta | None = None) -> None:
        if self._category != MessageCategory.NORMAL:
            raise ValueError(f"Can not force retry message with category {self._category}.")

        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.requeue(
            self._key,
            self.raw_payload,
            self.parameters._prepare_retry(
                next_retry=timedelta(seconds=0) if next_retry is None else next_retry,
            ),
        )

        self.__read_only = True
