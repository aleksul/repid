from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repid.connections import (
    DummyBucketing,
    DummyMessaging,
    RabbitMessaging,
    RedisBucketing,
    RedisMessaging,
)

if TYPE_CHECKING:
    from repid.connections import Bucketing, Messaging


CONNECTIONS_MAPPING: dict[str, type[Messaging]] = {
    "amqp://": RabbitMessaging,
    "amqps://": RabbitMessaging,
    "redis://": RedisMessaging,
    "rediss://": RedisMessaging,
    "dummy://": DummyMessaging,
}

BUCKETINGS_MAPPING: dict[str, type[Bucketing]] = {
    "redis://": RedisBucketing,
    "rediss://": RedisBucketing,
    "dummy://": DummyBucketing,
}


def _get_messaging_from_string(dsn: str) -> Messaging:
    global CONNECTIONS
    for prefix, conn in CONNECTIONS_MAPPING.items():
        if dsn.startswith(prefix):
            return conn(dsn)
    raise ValueError(f"Unsupported DSN: {dsn}")


def _get_bucketing_from_string(dsn: str) -> Bucketing:
    global BUCKETINGS
    for prefix, bucketing in BUCKETINGS_MAPPING.items():
        if dsn.startswith(prefix):
            return bucketing(dsn)
    raise ValueError(f"Unsupported DSN: {dsn}")


class Connection:
    __slots__ = ("messager", "args_bucketer", "results_bucketer")

    def __init__(
        self,
        messager: str,
        args_bucketer: str | None = None,
        results_bucketer: str | None = None,
    ) -> None:
        self.messager: Messaging
        self.args_bucketer: Bucketing | None
        self.results_bucketer: Bucketing | None

        object.__setattr__(
            self,
            "messager",
            _get_messaging_from_string(messager),
        )
        object.__setattr__(
            self,
            "args_bucketer",
            _get_bucketing_from_string(args_bucketer) if args_bucketer is not None else None,
        )
        object.__setattr__(
            self,
            "results_bucketer",
            _get_bucketing_from_string(results_bucketer) if results_bucketer is not None else None,
        )

    @property
    def _ab(self) -> Bucketing:  # args bucketer
        if self.args_bucketer is None:
            raise ValueError("Args bucketer is not configured.")
        return self.args_bucketer

    @property
    def _rb(self) -> Bucketing:  # results bucketer
        if self.results_bucketer is None:
            raise ValueError("Results bucketer is not configured.")
        return self.results_bucketer

    def __setattr__(self, __name: str, __value: Any) -> None:
        raise NotImplementedError
