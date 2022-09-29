import asyncio
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

from repid.middlewares import Middleware, MiddlewareWrapper

if TYPE_CHECKING:
    from repid.connections import BucketBrokerT, MessageBrokerT


@dataclass(frozen=True)
class Connection:
    message_broker: "MessageBrokerT"
    args_bucket_broker: Union["BucketBrokerT", None] = None
    results_bucket_broker: Union["BucketBrokerT", None] = None
    middleware: Middleware = field(default_factory=Middleware, init=False)

    def __post_init__(self) -> None:
        # if self.results_bucket_broker is not None and isinstance(
        #     self.results_bucket_broker.BUCKET_CLASS(data="42", started_when=42, finished_when=42),
        #     ResultBucketT,
        # ):
        #     raise ValueError(
        #         "Results bucketer's BUCKET_CLASS must be compatible with ResultBucketT type."
        #     )

        for proto in [self.message_broker, self.args_bucket_broker, self.results_bucket_broker]:
            if proto is None:
                continue
            for _, attr in inspect.getmembers(proto):
                if isinstance(attr, MiddlewareWrapper):
                    attr._repid_signal_emitter = self.middleware.emit_signal

    async def connect(self) -> None:
        await asyncio.gather(
            *[
                proto.connect()
                for proto in [
                    self.message_broker,
                    self.args_bucket_broker,
                    self.results_bucket_broker,
                ]
                if proto is not None
            ],
            return_exceptions=True
        )

    async def disconnect(self) -> None:
        await asyncio.gather(
            *[
                proto.disconnect()
                for proto in [
                    self.message_broker,
                    self.args_bucket_broker,
                    self.results_bucket_broker,
                ]
                if proto is not None
            ],
            return_exceptions=True
        )

    @property
    def _ab(self) -> "BucketBrokerT":  # args bucketer
        if self.args_bucket_broker is None:
            raise ValueError("Args bucketer is not configured.")
        return self.args_bucket_broker

    @property
    def _rb(self) -> "BucketBrokerT":  # results bucketer
        if self.results_bucket_broker is None:
            raise ValueError("Results bucketer is not configured.")
        return self.results_bucket_broker
