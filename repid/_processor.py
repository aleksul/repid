from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from repid._utils import _ArgsBucketInMessageId
from repid.actor import ActorData, ActorResult
from repid.logger import logger
from repid.middlewares import middleware_wrapper

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.data import ParametersT, RoutingKeyT
    from repid.data.protocols import ResultPropertiesT


class _Processor:
    __slots__ = ("_conn", "_processed")

    def __init__(self, _conn: Connection) -> None:
        self._conn = _conn
        self.actor_run._repid_signal_emitter = self._conn.middleware.emit_signal
        self._processed = 0

    async def get_payload(self, initial_payload: str) -> str:
        if _ArgsBucketInMessageId.check(initial_payload):
            bucket = await self._conn._ab.get_bucket(
                _ArgsBucketInMessageId.deconstruct(initial_payload),
            )
            if bucket is not None:
                return bucket.data
        return initial_payload

    @staticmethod
    @middleware_wrapper
    async def actor_run(
        actor: ActorData,
        key: RoutingKeyT,
        parameters: ParametersT,
        payload: str,
    ) -> ActorResult:
        time_limit = parameters.execution_timeout.total_seconds()

        logger_extra = {
            "actor_name": actor.name,
            "message_id": key.id_,
            "time_limit": time_limit,
        }

        result: Any = None
        success: bool
        exception = None

        logger.info("Running actor '{actor_name}' on message {message_id}.", extra=logger_extra)
        logger.debug("Time limit is set to {time_limit}.", extra=logger_extra)

        started_when = time.perf_counter_ns()

        try:
            args, kwargs = actor.converter.convert_inputs(payload)
            _result = await asyncio.wait_for(actor.fn(*args, **kwargs), timeout=time_limit)
            result = actor.converter.convert_outputs(_result)
        except Exception as exc:  # noqa: BLE001
            exception = exc
            success = False
            logger.exception(
                "Error inside of an actor '{actor_name}' on message {message_id}.",
                extra=logger_extra,
            )
        else:
            logger.info(
                "Actor '{actor_name}' finished successfully on message {message_id}.",
                extra=logger_extra,
            )
            success = True

        return ActorResult(
            data=result,
            success=success,
            exception=exception,
            started_when=started_when,
            finished_when=time.perf_counter_ns(),
        )

    async def report_to_broker(
        self,
        actor: ActorData,
        key: RoutingKeyT,
        payload: str,
        parameters: ParametersT,
        result: ActorResult,
    ) -> None:
        # rescheduling (retry)
        if not result.success and parameters.retries.already_tried < parameters.retries.max_amount:
            await self._conn.message_broker.requeue(
                key,
                payload,
                parameters._prepare_retry(actor.retry_policy(parameters.retries.already_tried + 1)),
            )
        # rescheduling (deferred)
        elif parameters.delay.defer_by is not None or parameters.delay.cron is not None:
            await self._conn.message_broker.requeue(
                key,
                payload,
                parameters._prepare_reschedule(),
            )
        # ack
        elif result.success:
            await self._conn.message_broker.ack(key)
        # nack
        else:
            await self._conn.message_broker.nack(key)

    async def set_result_bucket(
        self,
        result_params: ResultPropertiesT | None,
        result_actor: ActorResult,
    ) -> None:
        if result_params is None:
            return
        await self._conn._rb.store_bucket(
            result_params.id_,
            self._conn._rb.BUCKET_CLASS(  # type: ignore[call-arg]
                data=result_actor.data,
                started_when=result_actor.started_when,
                finished_when=result_actor.finished_when,
                success=result_actor.success,
                exception=str(result_actor.exception)
                if result_actor.exception is not None
                else None,
                timestamp=datetime.now(),
                ttl=result_params.ttl,
            ),
        )

    async def process(
        self,
        actor: ActorData,
        key: RoutingKeyT,
        payload: str,
        parameters: ParametersT,
    ) -> None:
        raw_payload = await self.get_payload(payload)
        result = await self.actor_run(actor, key, parameters, raw_payload)
        await self.report_to_broker(actor, key, payload, parameters, result)
        self._processed += 1
        await self.set_result_bucket(parameters.result, result)

    @property
    def processed(self) -> int:
        return self._processed  # pragma: no cover
