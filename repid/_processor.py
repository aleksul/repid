from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

import orjson

from repid.actor import ActorData, ActorResult
from repid.logger import logger
from repid.middlewares import middleware_wrapper

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.data import MessageT
    from repid.data.protocols import ResultPropertiesT


class _Processor:
    __slots__ = ("_conn", "_processed")

    def __init__(self, _conn: Connection) -> None:
        self._conn = _conn
        self.actor_run._repid_signal_emitter = self._conn.middleware.emit_signal
        self._processed = 0

    async def get_payload(self, initial_payload: str) -> str:
        if initial_payload.find("__repid_payload_id", 0, 20) != -1:
            bucket_id: str = orjson.loads(initial_payload).get("__repid_payload_id")
            bucket = await self._conn._ab.get_bucket(bucket_id)
            if bucket is not None:
                return bucket.data
        return initial_payload

    @staticmethod
    @middleware_wrapper
    async def actor_run(
        actor: ActorData,
        message: MessageT,
        args: list,
        kwargs: dict,
    ) -> ActorResult:
        time_limit = message.parameters.execution_timeout.total_seconds()

        logger_extra = dict(
            actor_name=actor.name,
            message_id=message.key.id_,
            time_limit=time_limit,
        )

        result: Any = None
        success: bool
        exception = None

        logger.info("Running actor '{actor_name}' on message {message_id}.", extra=logger_extra)
        logger.debug("Time limit is set to {time_limit}.", extra=logger_extra)

        started_when = time.perf_counter_ns()

        try:
            result = await asyncio.wait_for(actor.fn(*args, **kwargs), timeout=time_limit)
        except Exception as exc:
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
        message: MessageT,
        result: ActorResult,
    ) -> None:
        params = message.parameters
        retry_number = params.retries.already_tried + 1
        # rescheduling (retry)
        if not result.success and retry_number < params.retries.max_amount:
            await self._conn.message_broker.requeue(
                message.key,
                message.payload,
                params._prepare_retry(actor.retry_policy(retry_number)),
            )
        # rescheduling (deferred)
        elif params.delay.defer_by is not None or params.delay.cron is not None:
            await self._conn.message_broker.requeue(
                message.key,
                message.payload,
                params._prepare_reschedule(),
            )
        # ack
        elif result.success:
            await self._conn.message_broker.ack(message.key)
        # nack
        else:
            await self._conn.message_broker.nack(message.key)

    async def set_result_bucket(
        self,
        result_params: ResultPropertiesT | None,
        returns: str,
        result_actor: ActorResult,
    ) -> None:
        if result_params is None:
            return
        await self._conn._rb.store_bucket(
            result_params.id_,
            self._conn._rb.BUCKET_CLASS(  # type: ignore[call-arg]
                data=returns,
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

    async def process(self, actor: ActorData, message: MessageT) -> None:
        raw_payload = await self.get_payload(message.payload)
        args, kwargs = actor.converter.convert_inputs(raw_payload)
        result = await self.actor_run(actor, message, args, kwargs)
        await self.report_to_broker(actor, message, result)
        self._processed += 1
        returns = actor.converter.convert_outputs(result.data)
        await self.set_result_bucket(message.parameters.result, returns, result)

    @property
    def processed(self) -> int:
        return self._processed  # pragma: no cover
