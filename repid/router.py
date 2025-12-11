from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar, overload

from repid._utils import NotSet, asyncify
from repid.converter import DefaultConverter
from repid.data import ActorData, Channel, CorrelationId
from repid.dependencies._utils import validate_dependency
from repid.middlewares import ActorMiddlewareT, _compile_actor_middleware_pipeline

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from repid.asyncapi.models import OperationBindingsObject
    from repid.connections.abc import BaseMessageT, ReceivedMessageT
    from repid.converter import ConverterT
    from repid.data import Channel, ExternalDocs, Tag


class RoutingStrategyT(Protocol):
    def __call__(self, *, actor_name: str, **kwargs: Any) -> Callable[[BaseMessageT], bool]: ...


def topic_based_routing_strategy(*, actor_name: str, **_: Any) -> Callable[[BaseMessageT], bool]:
    def strategy(message: BaseMessageT) -> bool:
        if message.headers is None:
            return False
        return message.headers.get("topic") == actor_name

    return strategy


def catch_all_routing_strategy(*, actor_name: str, **_: Any) -> Callable[[BaseMessageT], bool]:  # noqa: ARG001
    def strategy(_: BaseMessageT) -> bool:
        return True

    return strategy


YourFunc = TypeVar("YourFunc", bound=Callable)


@dataclass(slots=True, kw_only=True, frozen=True)
class _ActorDefinition:
    router: Router
    fn: Callable[..., Coroutine[Any, Any, Any]]
    name: str | None
    confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"]
    routing_strategy: RoutingStrategyT
    channel: Channel | str | None
    middlewares: Sequence[ActorMiddlewareT] | None
    timeout: float | None
    title: str | None
    summary: str | None
    description: str | None
    run_in_process: bool | None
    pool_executor: Executor | None
    converter: type[ConverterT] | None
    security: Sequence[Any] | None
    tags: Sequence[Tag] | None
    external_docs: ExternalDocs | None
    bindings: OperationBindingsObject | None
    deprecated: bool
    correlation_id: CorrelationId | None
    fn_locals: dict[str, Any] | None


class Router:
    __slots__ = (
        "_definitions",
        "channel",
        "converter",
        "middlewares",
        "pool_executor",
        "run_in_process",
        "timeout",
    )

    def __init__(
        self,
        *,
        channel: str | Channel = NotSet,
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float = NotSet,
        run_in_process: bool = NotSet,
        pool_executor: Executor | None = NotSet,
        converter: type[ConverterT] = NotSet,
    ) -> None:
        self._definitions: list[_ActorDefinition] = []
        self.channel = channel
        self.middlewares = middlewares
        self.timeout = timeout
        self.run_in_process = run_in_process
        self.pool_executor = pool_executor
        self.converter = converter

    def include_router(self, router: Router) -> None:
        self._definitions.extend(router._definitions)

        # propagate defaults
        if router.channel is NotSet and self.channel is not NotSet:
            router.channel = self.channel

        if self.middlewares is not None:
            child_mws = router.middlewares
            if child_mws is None:
                router.middlewares = self.middlewares
            else:
                router.middlewares = tuple(self.middlewares) + tuple(child_mws)

        if router.timeout is NotSet and self.timeout is not NotSet:
            router.timeout = self.timeout

        if router.run_in_process is NotSet and self.run_in_process is not NotSet:
            router.run_in_process = self.run_in_process

        if router.pool_executor is NotSet and self.pool_executor is not NotSet:
            router.pool_executor = self.pool_executor

        if router.converter is NotSet and self.converter is not NotSet:
            router.converter = self.converter

    @property
    def _actors_per_channel_address(self) -> dict[str, list[ActorData]]:
        result: dict[str, list[ActorData]] = {}
        for actor in self.actors:
            if actor.channel_address not in result:
                result[actor.channel_address] = []
            result[actor.channel_address].append(actor)
        return result

    @property
    def channels(self) -> list[Channel]:
        channels: dict[str, Channel] = {}
        for definition in self._definitions:
            if definition.channel is None:
                if isinstance(definition.router.channel, Channel):
                    channel = definition.router.channel
                elif definition.router.channel is NotSet:
                    channel = Channel(address="default")
                else:
                    channel = Channel(address=definition.router.channel)
            elif isinstance(definition.channel, Channel):
                channel = definition.channel
            else:
                channel = Channel(address=definition.channel)
            if channel.address not in channels:
                channels[channel.address] = channel
                continue
            existing = channels[channel.address]
            if not any(
                (
                    existing.title,
                    existing.summary,
                    existing.description,
                    existing.bindings,
                    existing.external_docs,
                ),
            ) and any(
                (
                    channel.title,
                    channel.summary,
                    channel.description,
                    channel.bindings,
                    channel.external_docs,
                ),
            ):
                channels[channel.address] = channel
        return list(channels.values())

    @property
    def actors(self) -> list[ActorData]:
        actors: list[ActorData] = []
        for definition in self._definitions:
            fn = definition.fn
            name = definition.name
            confirmation_mode = definition.confirmation_mode
            routing_strategy = definition.routing_strategy
            channel = definition.channel
            middlewares = definition.middlewares
            timeout = definition.timeout
            title = definition.title
            summary = definition.summary
            description = definition.description
            run_in_process = definition.run_in_process
            pool_executor = definition.pool_executor
            converter = definition.converter
            security = definition.security
            tags = definition.tags
            external_docs = definition.external_docs
            bindings = definition.bindings
            deprecated = definition.deprecated
            correlation_id = definition.correlation_id
            fn_locals = definition.fn_locals

            if converter is None:
                converter = (
                    DefaultConverter
                    if definition.router.converter is NotSet
                    else definition.router.converter
                )

            if channel is None:
                channel = (
                    "default" if definition.router.channel is NotSet else definition.router.channel
                )

            channel_address = channel if isinstance(channel, str) else channel.address

            actual_name = name or fn.__name__

            actual_routing_strategy = routing_strategy(actor_name=actual_name)

            defaults_middlewares = definition.router.middlewares or []
            actor_middlewares = middlewares or []
            all_middlewares: list[ActorMiddlewareT] = [
                *defaults_middlewares,
                *actor_middlewares,
            ]

            composer = _compile_actor_middleware_pipeline(all_middlewares)

            async def middleware_pipeline(
                call_next: Callable[[ReceivedMessageT, ActorData], Coroutine[Any, Any, Any]],
                message: ReceivedMessageT,
                actor: ActorData,
                _composer: Callable = composer,
                _all_middlewares: list[ActorMiddlewareT] = all_middlewares,
            ) -> Any:
                if _all_middlewares:
                    final = _composer(call_next)
                    return await final(message, actor)
                return await call_next(message, actor)

            timeout_val = timeout
            if timeout_val is None:
                timeout_val = (
                    300.0 if definition.router.timeout is NotSet else definition.router.timeout
                )

            run_in_process_val = run_in_process
            if run_in_process_val is None:
                run_in_process_val = (
                    False
                    if definition.router.run_in_process is NotSet
                    else definition.router.run_in_process
                )

            pool_executor_val = pool_executor
            if pool_executor_val is None:
                pool_executor_val = (
                    None
                    if definition.router.pool_executor is NotSet
                    else definition.router.pool_executor
                )

            actor_data = ActorData(
                fn=asyncify(
                    fn,
                    run_in_process=run_in_process_val,
                    executor=pool_executor_val,
                ),
                name=actual_name,
                confirmation_mode=confirmation_mode,
                routing_strategy=actual_routing_strategy,
                middleware_pipeline=middleware_pipeline,
                channel_address=channel_address,
                timeout=timeout_val,
                converter=converter(fn, fn_locals=fn_locals, correlation_id=correlation_id),
                title=title,
                summary=summary or " ".join([part.capitalize() for part in fn.__name__.split("_")]),
                description=description or fn.__doc__,
                security=tuple(security) if security is not None else None,
                tags=tuple(tags) if tags is not None else None,
                external_docs=external_docs,
                bindings=bindings,
                deprecated=deprecated,
            )
            actors.append(actor_data)
        return actors

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
        correlation_id: CorrelationId | None = None,
    ) -> Callable[[YourFunc], YourFunc]: ...

    @overload
    def actor(
        self,
        fn: YourFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
        correlation_id: CorrelationId | None = None,
    ) -> YourFunc: ...

    def actor(
        self,
        fn: YourFunc | None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        run_in_process: bool | None = None,
        pool_executor: Executor | None = None,
        converter: type[ConverterT] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
        deprecated: bool = False,
        correlation_id: CorrelationId | None = None,
    ) -> YourFunc | Callable[[YourFunc], YourFunc]:
        """Actor decorator.

        Args:
            name (str | None, optional):
                actor's name.
                Used for routing a message to this actor, using the name as topic.
                Defaults to the name of your wrapped function.
            confirmation_mode (Literal["auto", "always_ack", "ack_first", "manual"], optional):
                How the message should be acknowledged. Defaults to "auto".
                - "auto": the message will be acknowledged automatically after successful processing,
                    and nacked on failure. If the message was already acted upon, no action will be taken.
                - "always_ack": the message will be acknowledged (if not acted upon) after both
                    successful and failed processing.
                - "ack_first": the message will be acknowledged before processing. May lead to lost
                    messages if the worker crashes or processing fails.
                - "manual": the message will not be acknowledged automatically. You must act on the
                    message inside of the actor. Failure to do so may lead to undefined behavior.
            routing_strategy (RoutingStrategyT, optional):
                A factory, that will create a routing strategy for the actor.
                Can be one of the built-in strategies, or provided by user.
                Messages are always routed to the first matching actor.
                Defaults to a topic-based routing strategy, where the actor's name is matched
                against the "topic" header of the message.
            channel (Channel | str | None, optional):
                AsyncAPI channel for this actor.
                Defaults to Router's default channel.
            middlewares (Sequence[ActorMiddlewareT] | None, optional):
                Sequence of middlewares to apply to this actor.
                If specified, concatenated with Router's default middlewares.
            timeout (float | None, optional):
                Time limit for processing a message, in seconds.
                If the actor does not complete within this time, the message will be rejected.
                If zero or inf+, no time limit is applied.
                Defaults to Router's default timeout.
            title (str | None, optional):
                Human-readable title for the actor.
            summary (str | None, optional):
                Brief summary of what the actor does.
                Defaults to wrapped function's name.
            description (str | None, optional):
                Detailed description of the actor's purpose.
                Defaults to wrapped function's docstring.
            run_in_process (bool | None, optional):
                If True, runs synchronous actors inside of a `ProcessPoolExecutor`
                instead of the default `ThreadPoolExecutor`.
                Defaults to Router's default run_in_process value.
                Has no effect for asynchronous actors.
            pool_executor (Executor | None, optional):
                Custom executor to run synchronous actors.
                If provided, overrides the run_in_process setting.
                Has no effect for asynchronous actors.
            converter (type[ConverterT] | None, optional):
                Class that decides how the arguments and return type
                should be validated & parsed.
                Defaults to Router's default converter.
            security (Sequence[Any] | None, optional):
                Security requirements for the actor to be displayed in AsyncAPI schema.
            tags (Sequence[Tag] | None, optional):
                Tags to categorize the actor in AsyncAPI schema.
            external_docs (ExternalDocs | None, optional):
                External documentation for the actor to be displayed in AsyncAPI schema.
            bindings (OperationBindingsObject | None, optional):
                Operation bindings for the actor, used to specify protocol-specific details.
            deprecated (bool, optional):
                Whether the actor is deprecated. Defaults to False.
            correlation_id (CorrelationId | None, optional):
                Correlation ID location descriptor for messages handled by the actor.

        Returns:
            YourFunc: your initial function.
        """

        if fn is None:
            return partial(  # type: ignore[return-value]
                self.actor,
                name=name,
                confirmation_mode=confirmation_mode,
                routing_strategy=routing_strategy,
                channel=channel,
                middlewares=middlewares,
                timeout=timeout,
                title=title,
                summary=summary,
                description=description,
                run_in_process=run_in_process,
                converter=converter,
                security=security,
                tags=tags,
                external_docs=external_docs,
                bindings=bindings,
                deprecated=deprecated,
                correlation_id=correlation_id,
            )

        fn_locals: dict[str, Any] | None = None
        current_frame = inspect.currentframe()
        if current_frame is not None:
            previous_frame = current_frame.f_back
            if previous_frame is not None:
                fn_locals = previous_frame.f_locals

        # Validate dependencies early to emit warnings at registration time
        signature = inspect.signature(
            fn,
            eval_str=True,
            locals=fn_locals,
            globals=fn.__globals__,
        )
        for p in signature.parameters.values():
            validate_dependency(p.annotation)

        self._definitions.append(
            _ActorDefinition(
                router=self,
                fn=fn,
                name=name,
                confirmation_mode=confirmation_mode,
                routing_strategy=routing_strategy,
                channel=channel,
                middlewares=middlewares,
                timeout=timeout,
                title=title,
                summary=summary,
                description=description,
                run_in_process=run_in_process,
                pool_executor=pool_executor,
                converter=converter,
                security=security,
                tags=tags,
                external_docs=external_docs,
                bindings=bindings,
                deprecated=deprecated,
                correlation_id=correlation_id,
                fn_locals=fn_locals,
            ),
        )
        return fn
