from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Coroutine, cast

from repid._utils import _ArgsBucketInMessageId, _NoAction
from repid.actor import ActorData, ActorResult
from repid.dependencies import Depends
from repid.dependencies import Message as MessageDependency
from repid.logger import logger
from repid.middlewares import middleware_wrapper

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.data import ParametersT, ResultBucketT, RoutingKeyT
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
        connection: Connection,
    ) -> ActorResult | None:
        time_limit = parameters.execution_timeout.total_seconds()

        logger_extra = {
            "actor_name": actor.name,
            "message_id": key.id_,
            "time_limit": time_limit,
        }

        result: str | None = None
        success: bool
        exception = None

        logger.info("Running actor '{actor_name}' on message {message_id}.", extra=logger_extra)
        logger.debug("Time limit is set to {time_limit}.", extra=logger_extra)

        started_when = time.time_ns()

        try:
            dependency_kwargs: dict[str, Any] = {}
            unresolved_dependencies: dict[str, Coroutine] = {}
            for dep_name, dep in actor.converter.dependencies.items():
                if dep is MessageDependency:
                    dependency_kwargs[dep_name] = MessageDependency(
                        key=key,
                        raw_payload=payload,
                        parameters=parameters,
                        _connection=connection,
                        _actor_data=actor,
                        _actor_processing_started_when=started_when,
                    )
                elif isinstance(dep, Depends):
                    unresolved_dependencies[dep_name] = dep.fn()
                else:
                    raise ValueError("Unsupported dependency argument.")

            unresolved_dependencies_names, unresolved_dependencies_values = (
                unresolved_dependencies.keys(),
                unresolved_dependencies.values(),
            )

            resolved = await asyncio.gather(*unresolved_dependencies_values)

            dependency_kwargs.update(dict(zip(unresolved_dependencies_names, resolved)))

            args, kwargs = actor.converter.convert_inputs(payload)
            _result = await asyncio.wait_for(
                actor.fn(*args, **kwargs, **dependency_kwargs),
                timeout=time_limit,
            )
            result = actor.converter.convert_outputs(_result)
        except _NoAction:
            return None
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
            finished_when=time.time_ns(),
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

        bucket: ResultBucketT

        if result_actor.success:
            bucket = self._conn._rb.BUCKET_CLASS(  # type: ignore[call-arg, assignment]
                data=cast(str, result_actor.data),
                started_when=result_actor.started_when,
                finished_when=result_actor.finished_when,
                success=result_actor.success,
                exception=None,
                timestamp=datetime.now(),
                ttl=result_params.ttl,
            )
        else:
            bucket = self._conn._rb.BUCKET_CLASS(  # type: ignore[call-arg, assignment]
                data=str(result_actor.exception),
                started_when=result_actor.started_when,
                finished_when=result_actor.finished_when,
                success=result_actor.success,
                exception=type(result_actor.exception).__name__,
                timestamp=datetime.now(),
                ttl=result_params.ttl,
            )

        await self._conn._rb.store_bucket(result_params.id_, bucket)

    async def process(
        self,
        actor: ActorData,
        key: RoutingKeyT,
        payload: str,
        parameters: ParametersT,
    ) -> None:
        raw_payload = await self.get_payload(payload)

        result = await self.actor_run(actor, key, parameters, raw_payload, self._conn)
        if result is None:  # actor has finished gracefully, but no action is required
            self._processed += 1
            return

        await self.report_to_broker(actor, key, payload, parameters, result)
        self._processed += 1
        await self.set_result_bucket(parameters.result, result)

    @property
    def processed(self) -> int:
        return self._processed  # pragma: no cover
