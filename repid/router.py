from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, replace
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar, overload

from repid._utils import NotSet, asyncify
from repid._utils.not_set import _NotSet
from repid.converter import DefaultConverter
from repid.data import (
    ActorData,
    Channel,
    CorrelationId,
    ManualActionT,
    OnErrorAutoT,
    OnErrorManualT,
    OnErrorT,
)
from repid.dependencies._utils import validate_dependency
from repid.middlewares import ActorMiddlewareT, _compile_actor_middleware_pipeline

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from repid.asyncapi.models import OperationBindingsObject
    from repid.connections.abc import BaseMessageT, ReceivedMessageT
    from repid.converter import ConverterT
    from repid.data import Channel, ExternalDocs, Tag
    from repid.data.message_schema import ActorMessageMetadata
    from repid.limits import ActorLimits, LimitPolicyT


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


def _channel_has_docs(channel: Channel) -> bool:
    return any(
        (
            channel.title,
            channel.summary,
            channel.description,
            channel.bindings,
            channel.external_docs,
        ),
    )


YourFunc = TypeVar("YourFunc", bound=Callable)
ExplicitFunc = TypeVar("ExplicitFunc", bound=Callable[..., Coroutine[Any, Any, ManualActionT]])


@dataclass(slots=True, kw_only=True, frozen=True)
class _ActorDefinition:
    router: Router
    fn: Callable[..., Coroutine[Any, Any, Any]]
    name: str | None
    confirmation_mode: Literal["auto", "always_ack", "ack_first", "manual", "manual_explicit"]
    routing_strategy: RoutingStrategyT
    channel: Channel | str | None
    limits: ActorLimits | None
    limit_policies: Sequence[LimitPolicyT]
    middlewares: Sequence[ActorMiddlewareT] | None
    timeout: float | None
    keep_alive: bool | float | None
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
    on_error: OnErrorT
    correlation_id: CorrelationId | None
    fn_locals: dict[str, Any] | None
    message_schema: ActorMessageMetadata | None


@dataclass(slots=True, kw_only=True, frozen=True)
class _IncludedRouter:
    router: Router


@dataclass(slots=True, kw_only=True, frozen=True)
class _RouterDefaults:
    channel: Channel | str | _NotSet
    limits: tuple[ActorLimits, ...]
    limit_policies: tuple[LimitPolicyT, ...]
    middlewares: tuple[ActorMiddlewareT, ...]
    timeout: float | _NotSet
    keep_alive: bool | float | _NotSet | None
    run_in_process: bool | _NotSet
    pool_executor: Executor | _NotSet | None
    converter: type[ConverterT] | _NotSet

    @classmethod
    def empty(cls) -> _RouterDefaults:
        return cls(
            channel=NotSet,
            limits=(),
            limit_policies=(),
            middlewares=(),
            timeout=NotSet,
            keep_alive=NotSet,
            run_in_process=NotSet,
            pool_executor=NotSet,
            converter=NotSet,
        )


@dataclass(slots=True, kw_only=True, frozen=True)
class _MaterializedRouter:
    actors: list[ActorData]
    channels: list[Channel]
    _actors_per_channel_address: dict[str, list[ActorData]]


class Router:
    __slots__ = (
        "_entries",
        "channel",
        "converter",
        "keep_alive",
        "limit_policies",
        "limits",
        "middlewares",
        "pool_executor",
        "run_in_process",
        "timeout",
    )

    def __init__(
        self,
        *,
        channel: str | Channel = NotSet,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float = NotSet,
        keep_alive: bool | float | None = NotSet,
        run_in_process: bool = NotSet,
        pool_executor: Executor | None = NotSet,
        converter: type[ConverterT] = NotSet,
    ) -> None:
        self._entries: list[_ActorDefinition | _IncludedRouter] = []
        self.channel = channel
        self.limits = limits
        self.limit_policies = tuple(limit_policies)
        self.middlewares = middlewares
        self.timeout = timeout
        self.keep_alive = keep_alive
        self.run_in_process = run_in_process
        self.pool_executor = pool_executor
        self.converter = converter

    def include_router(self, router: Router) -> None:
        if router is self or router._contains_router(self):
            raise ValueError("Including this router would create a cycle.")

        if any(
            isinstance(entry, _IncludedRouter) and entry.router is router for entry in self._entries
        ):
            return

        self._entries.append(_IncludedRouter(router=router))

    def _contains_router(self, router: Router, seen: set[int] | None = None) -> bool:
        if seen is None:
            seen = set()
        if id(self) in seen:
            return False
        seen.add(id(self))

        for entry in self._entries:
            if isinstance(entry, _ActorDefinition):
                continue
            if entry.router is router or entry.router._contains_router(router, seen):
                return True
        return False

    def _materialize(self) -> _MaterializedRouter:
        actors: list[ActorData] = []
        channels: dict[str, Channel] = {}
        self._materialize_into(
            actors=actors,
            channels=channels,
            defaults=_RouterDefaults.empty(),
        )

        actors_per_channel_address: dict[str, list[ActorData]] = {}
        for actor in actors:
            if actor.channel_address not in actors_per_channel_address:
                actors_per_channel_address[actor.channel_address] = []
            actors_per_channel_address[actor.channel_address].append(actor)

        return _MaterializedRouter(
            actors=actors,
            channels=list(channels.values()),
            _actors_per_channel_address=actors_per_channel_address,
        )

    def _materialize_into(
        self,
        *,
        actors: list[ActorData],
        channels: dict[str, Channel],
        defaults: _RouterDefaults,
    ) -> None:
        current_defaults = self._merge_defaults(defaults)
        for entry in self._entries:
            if isinstance(entry, _IncludedRouter):
                entry.router._materialize_into(
                    actors=actors,
                    channels=channels,
                    defaults=current_defaults,
                )
            else:
                actor_data, channel = self._materialize_actor(entry, current_defaults)
                actors.append(actor_data)
                self._add_channel(channels, channel)

    def _merge_defaults(self, defaults: _RouterDefaults) -> _RouterDefaults:
        return _RouterDefaults(
            channel=(defaults.channel if isinstance(self.channel, _NotSet) else self.channel),
            limits=(*defaults.limits, self.limits) if self.limits is not None else defaults.limits,
            limit_policies=(*defaults.limit_policies, *self.limit_policies),
            middlewares=(
                defaults.middlewares
                if self.middlewares is None
                else (*defaults.middlewares, *self.middlewares)
            ),
            timeout=defaults.timeout if isinstance(self.timeout, _NotSet) else self.timeout,
            keep_alive=(
                defaults.keep_alive if isinstance(self.keep_alive, _NotSet) else self.keep_alive
            ),
            run_in_process=(
                defaults.run_in_process
                if isinstance(self.run_in_process, _NotSet)
                else self.run_in_process
            ),
            pool_executor=(
                defaults.pool_executor
                if isinstance(self.pool_executor, _NotSet)
                else self.pool_executor
            ),
            converter=(
                defaults.converter if isinstance(self.converter, _NotSet) else self.converter
            ),
        )

    @staticmethod
    def _add_channel(channels: dict[str, Channel], channel: Channel) -> None:
        """Register a channel, merging into an already-registered twin.

        Merge precedence when both sides declare the same address:

        - distinct non-``None`` limits objects are a conflict and raise;
        - custom limit policies compose and deduplicate by identity;
        - otherwise the side carrying AsyncAPI docs (title, summary, ...)
          provides the channel identity, defaulting to the earlier one;
        - limits and backpressure are taken from whichever side defines them.
        """
        if channel.address not in channels:
            channels[channel.address] = channel
            return

        existing = channels[channel.address]
        if (
            existing.limits is not None
            and channel.limits is not None
            and existing.limits is not channel.limits
        ):
            raise ValueError(f"Conflicting limits for channel {channel.address!r}.")
        # Keep whichever side carries docs, then merge limits from whichever
        # side defines them.
        selected = (
            channel if _channel_has_docs(channel) and not _channel_has_docs(existing) else existing
        )
        policies = (*existing.limit_policies, *channel.limit_policies)
        channels[channel.address] = replace(
            selected,
            limits=existing.limits if existing.limits is not None else channel.limits,
            limit_policies=tuple({id(policy): policy for policy in policies}.values()),
        )

    def _materialize_actor(
        self,
        definition: _ActorDefinition,
        defaults: _RouterDefaults,
    ) -> tuple[ActorData, Channel]:
        converter_cls = (
            definition.converter
            if definition.converter is not None
            else (
                DefaultConverter if isinstance(defaults.converter, _NotSet) else defaults.converter
            )
        )

        channel_obj = self._resolve_channel(definition.channel, defaults)
        channel_address = channel_obj.address

        actual_name = definition.name or definition.fn.__name__

        actual_routing_strategy = definition.routing_strategy(actor_name=actual_name)

        actor_middlewares = definition.middlewares or []
        all_middlewares: list[ActorMiddlewareT] = [
            *defaults.middlewares,
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

        timeout_val = (
            definition.timeout
            if definition.timeout is not None
            else (300.0 if isinstance(defaults.timeout, _NotSet) else defaults.timeout)
        )
        keep_alive_val = (
            definition.keep_alive
            if definition.keep_alive is not None
            else (True if isinstance(defaults.keep_alive, _NotSet) else defaults.keep_alive)
        )
        run_in_process_val = (
            definition.run_in_process
            if definition.run_in_process is not None
            else (
                False if isinstance(defaults.run_in_process, _NotSet) else defaults.run_in_process
            )
        )
        pool_executor_val = (
            definition.pool_executor
            if definition.pool_executor is not None
            else (None if isinstance(defaults.pool_executor, _NotSet) else defaults.pool_executor)
        )

        actor_data = ActorData(
            fn=asyncify(
                definition.fn,
                run_in_process=run_in_process_val,
                executor=pool_executor_val,
            ),
            name=actual_name,
            confirmation_mode=definition.confirmation_mode,
            routing_strategy=actual_routing_strategy,
            middleware_pipeline=middleware_pipeline,
            channel_address=channel_address,
            limits=(
                (*defaults.limits, definition.limits)
                if definition.limits is not None
                else defaults.limits
            ),
            limit_policies=(*defaults.limit_policies, *definition.limit_policies),
            timeout=timeout_val,
            keep_alive=keep_alive_val,
            converter=converter_cls(
                definition.fn,
                fn_locals=definition.fn_locals,
                correlation_id=definition.correlation_id,
            ),
            title=definition.title,
            summary=definition.summary
            or " ".join([part.capitalize() for part in definition.fn.__name__.split("_")]),
            description=definition.description or definition.fn.__doc__,
            security=tuple(definition.security) if definition.security is not None else None,
            tags=tuple(definition.tags) if definition.tags is not None else None,
            external_docs=definition.external_docs,
            bindings=definition.bindings,
            deprecated=definition.deprecated,
            on_error=definition.on_error,
            message_schema=definition.message_schema,
        )
        return actor_data, channel_obj

    @staticmethod
    def _resolve_channel(
        channel: Channel | str | None,
        defaults: _RouterDefaults,
    ) -> Channel:
        fallback_channel: Channel | str = "default"
        resolved_channel = (
            channel
            if channel is not None
            else (fallback_channel if isinstance(defaults.channel, _NotSet) else defaults.channel)
        )

        if isinstance(resolved_channel, Channel):
            return resolved_channel
        return Channel(address=resolved_channel)

    @property
    def _actors_per_channel_address(self) -> dict[str, list[ActorData]]:
        return self._materialize()._actors_per_channel_address

    @property
    def channels(self) -> list[Channel]:
        return self._materialize().channels

    @property
    def actors(self) -> list[ActorData]:
        return self._materialize().actors

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorAutoT = "nack",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> Callable[[YourFunc], YourFunc]: ...

    @overload
    def actor(
        self,
        fn: YourFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["auto"] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorAutoT = "nack",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> YourFunc: ...

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["always_ack", "ack_first"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        message_schema: ActorMessageMetadata | None = None,
    ) -> Callable[[YourFunc], YourFunc]: ...

    @overload
    def actor(
        self,
        fn: YourFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["always_ack", "ack_first"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        message_schema: ActorMessageMetadata | None = None,
    ) -> YourFunc: ...

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["manual"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorManualT = "no_action",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> Callable[[YourFunc], YourFunc]: ...

    @overload
    def actor(
        self,
        fn: YourFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["manual"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorManualT = "no_action",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> YourFunc: ...

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["manual_explicit"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorManualT = "no_action",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> Callable[[ExplicitFunc], ExplicitFunc]: ...

    @overload
    def actor(
        self,
        fn: ExplicitFunc,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal["manual_explicit"],
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorManualT = "no_action",
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> ExplicitFunc: ...

    def actor(
        self,
        fn: YourFunc | ExplicitFunc | None = None,
        /,
        name: str | None = None,
        *,
        confirmation_mode: Literal[
            "auto",
            "always_ack",
            "ack_first",
            "manual",
            "manual_explicit",
        ] = "auto",
        routing_strategy: RoutingStrategyT = topic_based_routing_strategy,
        channel: Channel | str | None = None,
        limits: ActorLimits | None = None,
        limit_policies: Sequence[LimitPolicyT] = (),
        middlewares: Sequence[ActorMiddlewareT] | None = None,
        timeout: float | None = None,
        keep_alive: bool | float | None = None,
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
        on_error: OnErrorAutoT | OnErrorManualT | None = None,
        correlation_id: CorrelationId | None = None,
        message_schema: ActorMessageMetadata | None = None,
    ) -> (
        YourFunc
        | ExplicitFunc
        | Callable[[YourFunc], YourFunc]
        | Callable[[ExplicitFunc], ExplicitFunc]
    ):
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
            limits (ActorLimits | None, optional): Built-in numeric execution
                limits composed with enclosing routers.
            limit_policies (Sequence[LimitPolicyT], optional): Application-defined
                limit policies composed with enclosing routers.
            middlewares (Sequence[ActorMiddlewareT] | None, optional):
                Sequence of middlewares to apply to this actor.
                If specified, concatenated with Router's default middlewares.
            timeout (float | None, optional):
                Time limit for processing a message, in seconds.
                If the actor does not complete within this time, the message will be rejected.
                If zero or inf+, no time limit is applied.
                Defaults to Router's default timeout.
            keep_alive (bool | float | None, optional):
                Whether the actor should periodically inform the broker that it's still alive.
                If a float is provided, it will override the broker's default keep-alive interval.
                Defaults to True, meaning the broker's default interval will be used.
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
            on_error (OnErrorT, optional):
                Controls what happens to the message when the actor raises an exception and
                `confirmation_mode="auto"`. Can be ``"nack"`` (discard / dead-letter, the default),
                ``"reject"`` (place back into the queue), or a callable that receives the
                exception instance and returns one of those two literals - enabling
                per-exception-type routing (e.g. nack ``ValidationError``, reject transient
                errors). Has no effect when `confirmation_mode` is anything but ``"auto"``.
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
                limits=limits,
                limit_policies=limit_policies,
                middlewares=middlewares,
                timeout=timeout,
                keep_alive=keep_alive,
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
                on_error=on_error,
                correlation_id=correlation_id,
                message_schema=message_schema,
            )

        if run_in_process is True and pool_executor is not None:
            raise ValueError("Specify either 'run_in_process' or 'pool_executor', not both.")

        if confirmation_mode in ("ack_first", "always_ack") and on_error is not None:
            raise ValueError(
                "The 'on_error' parameter is not compatible with 'ack_first' or 'always_ack' "
                "confirmation modes, as the message will always be acknowledged.",
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

        if on_error is None:
            on_error = "no_action" if confirmation_mode in ("manual", "manual_explicit") else "nack"

        self._entries.append(
            _ActorDefinition(
                router=self,
                fn=fn,
                name=name,
                confirmation_mode=confirmation_mode,
                routing_strategy=routing_strategy,
                channel=channel,
                limits=limits,
                limit_policies=limit_policies,
                middlewares=middlewares,
                timeout=timeout,
                keep_alive=keep_alive,
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
                on_error=on_error,
                correlation_id=correlation_id,
                fn_locals=fn_locals,
                message_schema=message_schema,
            ),
        )
        return fn
