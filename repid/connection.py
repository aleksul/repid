import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

from repid.data.protocols import ResultBucketT
from repid.middlewares import Middleware

if TYPE_CHECKING:
    from repid.connections import BucketBrokerT, MessageBrokerT


@dataclass(frozen=True)
class Connection:
    message_broker: "MessageBrokerT"
    args_bucket_broker: Union["BucketBrokerT", None] = None
    results_bucket_broker: Union["BucketBrokerT", None] = None
    middleware: Middleware = field(default_factory=Middleware, init=False)
    is_open: bool = field(default=False, init=False)

    async def connect(self) -> None:
        """Connect to all set brokers. Marks connection as open."""
        await asyncio.gather(
            *[
                broker.connect()
                for broker in [
                    self.message_broker,
                    self.args_bucket_broker,
                    self.results_bucket_broker,
                ]
                if broker is not None
            ]
        )
        object.__setattr__(self, "is_open", True)

    async def disconnect(self) -> None:
        """Disconnect from all set brokers. Marks connection as closed."""
        await asyncio.gather(
            *[
                broker.disconnect()
                for broker in [
                    self.message_broker,
                    self.args_bucket_broker,
                    self.results_bucket_broker,
                ]
                if broker is not None
            ]
        )
        object.__setattr__(self, "is_open", False)

    @property
    def _ab(self) -> "BucketBrokerT":
        """For internal use. Shortcut to get args bucket broker
        or raise an error, if it isn't set."""
        if self.args_bucket_broker is None:
            raise ValueError("Args bucket broker is not configured.")
        return self.args_bucket_broker

    @property
    def _rb(self) -> "BucketBrokerT":
        """For internal use. Shortcut to get result bucket broker
        or raise an error, if it isn't set."""
        if self.results_bucket_broker is None:
            raise ValueError("Results bucket broker is not configured.")
        return self.results_bucket_broker

    def __post_init__(self) -> None:
        # test result bucket broker has proper BUCKET_CLASS
        if self.results_bucket_broker is not None:
            try:
                test_subject = self.results_bucket_broker.BUCKET_CLASS(  # type: ignore[call-arg]
                    data="",
                    started_when=123,
                    finished_when=123,
                    success=True,
                    exception=None,
                )
                assert isinstance(test_subject, ResultBucketT)
            except Exception as exc:
                raise ValueError(
                    "Results bucket broker's BUCKET_CLASS must be compatible with ResultBucketT."
                ) from exc

        # set _signal_emitter for every submitted protocol
        for broker in [self.message_broker, self.args_bucket_broker, self.results_bucket_broker]:
            if broker is None:
                continue
            broker._signal_emitter = self.middleware.emit_signal
