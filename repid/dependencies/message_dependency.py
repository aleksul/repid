from __future__ import annotations

import time
from datetime import datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, Awaitable, Callable, NoReturn

from repid._asyncify import asyncify
from repid._utils import _NoAction
from repid.dependencies.protocols import DependencyKind
from repid.message import Message

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.connection import Connection
    from repid.data.protocols import ParametersT, RoutingKeyT
    from repid.dependencies.resolver_context import ResolverContext


class MessageDependency(Message):
    __repid_dependency__ = DependencyKind.DIRECT

    __slots__ = (
        "_actor_data",
        "_actor_processing_started_when",
        "_callbacks",
        "__lazy_result_callback",
        "__result_success",
        "__result_data",
        "__result_exception",
    )

    def __init__(
        self,
        *,
        key: RoutingKeyT,
        raw_payload: str,
        parameters: ParametersT,
        _connection: Connection,
    ) -> None:
        super().__init__(
            key=key,
            raw_payload=raw_payload,
            parameters=parameters,
            _connection=_connection,
        )
        self._actor_data: ActorData
        self._actor_processing_started_when: int
        self._callbacks: list[Callable[[], Awaitable]] = []
        self.__lazy_result_callback: Callable[[], None] = lambda: None
        self.__result_success: bool | None = None
        self.__result_data: str | None = None
        self.__result_exception: Exception | None = None

    @classmethod
    def construct_as_dependency(cls, *, context: ResolverContext) -> MessageDependency:
        # Construct message from resolver context
        instance = cls(
            key=context.message_key,
            raw_payload=context.message_raw_payload,
            parameters=context.message_parameters,
            _connection=context.connection,
        )
        instance._actor_data = context.actor_data
        instance._actor_processing_started_when = context.actor_processing_started_when
        return instance

    async def resolve(self) -> MessageDependency:
        return self

    def add_callback(self, fn: Callable[[], Any | Awaitable[Any]]) -> None:
        self._callbacks.append(asyncify(fn))

    def set_result(self, result: Any) -> None:
        if self.parameters.result is None:
            raise ValueError("parameters.result is not set.")

        if (rbb := self._connection.results_bucket_broker) is None:
            raise ValueError("Results bucket broker is not configured.")

        data = self._actor_data.converter.convert_outputs(result)

        self.__result_success = True
        self.__result_data = data
        self.__result_exception = None

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

        self.__result_success = False
        self.__result_data = None
        self.__result_exception = exc

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
        await super().ack()
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else True,
            data=self.__result_data,
            exception=self.__result_exception,
        )

    async def nack(self) -> NoReturn:
        await super().nack()
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else False,
            data=self.__result_data,
            exception=self.__result_exception,
        )

    async def reject(self) -> NoReturn:
        await super().reject()
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else False,
            data=self.__result_data,
            exception=self.__result_exception,
        )

    async def reschedule(self) -> NoReturn:
        await super().reschedule()
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else True,
            data=self.__result_data,
            exception=self.__result_exception,
        )

    async def retry(self, next_retry: timedelta | None = None) -> NoReturn:
        await super().retry(
            next_retry=(
                self._actor_data.retry_policy(
                    retry_number=self.parameters.retries.already_tried + 1,
                )
                if next_retry is None
                else next_retry
            ),
        )
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else False,
            data=self.__result_data,
            exception=self.__result_exception,
        )

    async def force_retry(self, next_retry: timedelta | None = None) -> NoReturn:
        await super().force_retry(
            next_retry=(
                self._actor_data.retry_policy(
                    retry_number=self.parameters.retries.already_tried + 1,
                )
                if next_retry is None
                else next_retry
            ),
        )
        await self.__execute_callbacks()
        raise _NoAction(
            success=self.__result_success if self.__result_success is not None else False,
            data=self.__result_data,
            exception=self.__result_exception,
        )
