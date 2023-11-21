from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from repid.main import Repid

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.data.protocols import ParametersT, RoutingKeyT


class Message:
    __slots__ = (
        "key",
        "raw_payload",
        "parameters",
        "_connection",
        "__read_only",
    )

    def __init__(
        self,
        *,
        key: RoutingKeyT,
        raw_payload: str,
        parameters: ParametersT,
        _connection: Connection | None = None,
    ) -> None:
        self.key = key
        self.raw_payload = raw_payload
        self.parameters = parameters
        self._connection = _connection or Repid.get_magic_connection()
        self.__read_only = False

    @property
    def read_only(self) -> bool:
        return self.__read_only

    async def ack(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.ack(self.key)

        self.__read_only = True

    async def nack(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.nack(self.key)

        self.__read_only = True

    async def reject(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.reject(self.key)

        self.__read_only = True

    async def reschedule(self) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.requeue(
            self.key,
            self.raw_payload,
            self.parameters._prepare_reschedule(),
        )

        self.__read_only = True

    async def retry(self, next_retry: timedelta | None = None) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        if self.parameters.retries.already_tried >= self.parameters.retries.max_amount:
            raise ValueError("Max retry limit reached.")

        await self._connection.message_broker.requeue(
            self.key,
            self.raw_payload,
            self.parameters._prepare_retry(
                next_retry=timedelta(seconds=0) if next_retry is None else next_retry,
            ),
        )

        self.__read_only = True

    async def force_retry(self, next_retry: timedelta | None = None) -> None:
        if self.__read_only:
            raise ValueError("Message is read only.")

        await self._connection.message_broker.requeue(
            self.key,
            self.raw_payload,
            self.parameters._prepare_retry(
                next_retry=timedelta(seconds=0) if next_retry is None else next_retry,
            ),
        )

        self.__read_only = True
