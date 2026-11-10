from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from repid._keep_alive import _keep_message_alive

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData, ActorExecutionContext

logger = logging.getLogger("repid")


ActorResultT = Any


async def _actor_execution(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
    args, kwargs = await actor.converter.convert_inputs(
        message=message,
        actor=actor,
        actor_context=actor_context,
    )
    return await actor.fn(*args, **kwargs)


async def _apply_confirmation_action(message: ReceivedMessageT, action: object) -> bool:
    if action == "reject":
        await message.reject()
    elif action == "nack":
        await message.nack()
    elif action == "ack":
        await message.ack()
    else:
        return False
    return True


async def _actor_execution_with_confirmation(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
    """Wraps `_actor_execution` to ack/nack the message immediately after the actor fn runs,
    so middlewares unwinding above this leaf can observe `message.action`."""
    timeout = actor.timeout
    try:
        if timeout is None or timeout <= 0 or timeout == float("inf"):
            result = await _actor_execution(message, actor, actor_context)
        else:
            async with asyncio.timeout(timeout):
                result = await _actor_execution(message, actor, actor_context)
    except Exception as exc:
        if message.is_acted_on:
            raise
        if actor.confirmation_mode in ("auto", "manual", "manual_explicit"):
            error_action = (
                actor.on_error if isinstance(actor.on_error, str) else actor.on_error(exc)
            )
            await _apply_confirmation_action(message, error_action)
        elif actor.confirmation_mode == "always_ack":
            await message.ack()
        raise

    if message.is_acted_on:
        return result
    if actor.confirmation_mode in ("auto", "always_ack"):
        await message.ack()
    elif actor.confirmation_mode == "manual_explicit":
        if await _apply_confirmation_action(message, result) or result == "no_action":
            return result
        raise ValueError(
            f"Actor '{actor.name}' with confirmation_mode='manual_explicit' "
            f"returned an invalid value: {result!r}. Expected one of: "
            "'ack', 'nack', 'reject', 'no_action'.",
        )
    return result


async def _actor_run(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> ActorResultT | Exception:
    interval = None
    if actor.keep_alive is not False:
        interval = (
            actor.keep_alive
            if isinstance(actor.keep_alive, (int, float)) and not isinstance(actor.keep_alive, bool)
            else message.keep_alive_interval
        )
    if interval is None:
        return await _run_actor_pipeline(actor, message, actor_context)

    async with _keep_message_alive(message, interval, actor_context.server):
        return await _run_actor_pipeline(actor, message, actor_context)


async def _run_actor_pipeline(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> ActorResultT | Exception:
    if (
        not message.is_acted_on  # theoretically a server can automatically ack the message on receive
        and actor.confirmation_mode == "ack_first"
    ):
        await message.ack()

    logger_extra = {
        "actor_name": actor.name,
        "time_limit": actor.timeout,
        "message_id": message.message_id,
    }

    exception = None
    result = None

    leaf = partial(
        _actor_execution_with_confirmation,
        actor_context=actor_context,
    )

    try:
        result = await actor.middleware_pipeline(leaf, message, actor)
    except Exception as exc:
        exception = exc
        logger.debug("actor.run.error", extra=logger_extra, exc_info=exc)
    else:
        logger.debug("actor.run.success", extra=logger_extra)

    if not message.is_acted_on and actor.confirmation_mode == "manual":
        logger.warning("actor.ack.manual.unacknowledged", extra=logger_extra)

    return exception if exception is not None else result
