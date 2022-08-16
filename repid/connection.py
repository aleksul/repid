from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repid.protocols import Messaging, Bucketing

from repid.connections import dummy, rabbitmq, redis

CONNECTIONS_MAPPING: dict[str, type[Messaging]] = {
    "amqp://": rabbitmq.RabbitMessaging,
    "amqps://": rabbitmq.RabbitMessaging,
    "redis://": redis.RedisMessaging,
    "rediss://": redis.RedisMessaging,
    "dummy://": dummy.DummyMessaging,
    # "kafka://": KafkaMessaging,
    # "nats://": NatsMessaging,
}

BUCKETINGS_MAPPING: dict[str, type[Bucketing]] = {
    "redis://": redis.RedisBucketing,
    "rediss://": redis.RedisBucketing,
    "dummy://": dummy.DummyBucketing,
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
    def ab(self) -> Bucketing:  # args bucketer
        if self.args_bucketer is None:
            raise ValueError("Args bucketer is not configured.")
        return self.args_bucketer

    @property
    def rb(self) -> Bucketing:  # results bucketer
        if self.results_bucketer is None:
            raise ValueError("Results bucketer is not configured.")
        return self.results_bucketer

    def __setattr__(self, __name: str, __value: Any) -> None:
        raise NotImplementedError
