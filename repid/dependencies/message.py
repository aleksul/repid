from __future__ import annotations

import time
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Awaitable, Callable, NoReturn

from repid._asyncify import asyncify
from repid._utils import _NoAction

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.connection import Connection
    from repid.data.protocols import ParametersT, RoutingKeyT


class Message:
    __repid_dependency__ = True

    __slots__ = (
        "key",
        "raw_payload",
        "parameters",
        "_connection",
        "_actor_data",
        "_actor_processing_started_when",
        "_callbacks",
        "__lazy_result_callback",
    )

    def __init__(
        self,
        key: RoutingKeyT,
        raw_payload: str,
        parameters: ParametersT,
        _connection: Connection,
        _actor_data: ActorData,
        _actor_processing_started_when: int,
    ) -> None:
        self.key = key
        self.raw_payload = raw_payload
        self.parameters = parameters
        self._connection = _connection
        self._actor_data = _actor_data
        self._actor_processing_started_when = _actor_processing_started_when
        self._callbacks: list[Callable[[], Awaitable]] = []
        self.__lazy_result_callback: Callable[[], None] = lambda: None

    def add_callback(self, fn: Callable[[], Any | Awaitable[Any]]) -> None:
        self._callbacks.append(asyncify(fn))

    def set_result(self, result: Any) -> None:
        if self.parameters.result is None:
            raise ValueError("parameters.result is not set.")

        if (rbb := self._connection.results_bucket_broker) is None:
            raise ValueError("Results bucket broker is not configured.")

        data = self._actor_data.converter.convert_outputs(result)

        async def _inner() -> None:
            await rbb.store_bucket(
                id_=self.parameters.result.id_,  # type: ignore[union-attr]
                payload=rbb.BUCKET_CLASS(  # type: ignore[call-arg]
                    data=data,
                    started_when=self._actor_processing_started_when,
                    finished_when=time.time_ns(),
                    success=True,
                    exception=None,
                    timestamp=datetime.now(),
                    ttl=self.parameters.result.ttl,  # type: ignore[union-attr]
                ),
            )

        self.__lazy_result_callback = partial(self._callbacks.insert, len(self._callbacks), _inner)

    def set_exception(self, exc: Exception) -> None:
        if self.parameters.result is None:
            raise ValueError("parameters.result is not set.")

        if (rbb := self._connection.results_bucket_broker) is None:
            raise ValueError("Results bucket broker is not configured.")

        async def _inner() -> None:
            await rbb.store_bucket(
                id_=self.parameters.result.id_,  # type: ignore[union-attr]
                payload=rbb.BUCKET_CLASS(  # type: ignore[call-arg]
                    data=str(exc),
                    started_when=self._actor_processing_started_when,
                    finished_when=time.time_ns(),
                    success=False,
                    exception=type(exc).__name__,
                    timestamp=datetime.now(),
                    ttl=self.parameters.result.ttl,  # type: ignore[union-attr]
                ),
            )

        self.__lazy_result_callback = partial(self._callbacks.insert, len(self._callbacks), _inner)

    async def __execute_callbacks(self) -> None:
        self.__lazy_result_callback()
        [await c() for c in self._callbacks]  # execute in order

    async def ack(self) -> NoReturn:
        await self._connection.message_broker.ack(self.key)
        await self.__execute_callbacks()
        raise _NoAction

    async def nack(self) -> NoReturn:
        await self._connection.message_broker.nack(self.key)
        await self.__execute_callbacks()
        raise _NoAction

    async def reject(self) -> NoReturn:
        await self._connection.message_broker.reject(self.key)
        await self.__execute_callbacks()
        raise _NoAction

    async def reschedule(self) -> NoReturn:
        await self._connection.message_broker.requeue(
            self.key,
            self.raw_payload,
            self.parameters._prepare_reschedule(),
        )
        await self.__execute_callbacks()
        raise _NoAction

    async def force_retry(self) -> NoReturn:
        await self._connection.message_broker.requeue(
            self.key,
            self.raw_payload,
            self.parameters._prepare_retry(
                self._actor_data.retry_policy(
                    retry_number=self.parameters.retries.already_tried + 1,
                ),
            ),
        )
        await self.__execute_callbacks()
        raise _NoAction
