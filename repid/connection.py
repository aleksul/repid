import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

from repid.config import Config
from repid.data.protocols import ResultBucketT
from repid.middlewares import Middleware

if TYPE_CHECKING:
    from repid.connections import BucketBrokerT, MessageBrokerT


@dataclass(frozen=True)
class Connection:
    """Connection dataclass represents a combination of connections to a message broker and bucket brokers."""

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
            ],
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
            ],
        )
        object.__setattr__(self, "is_open", False)

    @property
    def _ab(self) -> "BucketBrokerT":
        """For internal use.

        Shortcut to get args bucket broker or raise an error, if it isn't set.
        """
        if self.args_bucket_broker is None:
            raise ValueError("Args bucket broker is not configured.")
        return self.args_bucket_broker

    @property
    def _rb(self) -> "BucketBrokerT":
        """For internal use.

        Shortcut to get result bucket broker or raise an error, if it isn't set.
        """
        if self.results_bucket_broker is None:
            raise ValueError("Results bucket broker is not configured.")
        return self.results_bucket_broker

    def __post_init__(self) -> None:
        """Magic method that is called after the initialization of the object.

        If `results_bucket_broker` is not `None`, it tries to create an instance of the
        `BUCKET_CLASS` with some dummy data to check if it is compatible with the `ResultBucketT` class.
        If it is not compatible, it raises a `ValueError`.

        It then sets middleware signal emitter for every submitted broker.
        """
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
                if not isinstance(test_subject, ResultBucketT):
                    raise  # pragma: no cover
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    "Results bucket broker's BUCKET_CLASS must be compatible with ResultBucketT.",
                ) from exc

        # set _signal_emitter for every submitted protocol
        for broker in [self.message_broker, self.args_bucket_broker, self.results_bucket_broker]:
            if broker is None:
                continue
            broker._signal_emitter = self.middleware.emit_signal

    def _update_from_config(self) -> None:
        """For internal use.

        Manually update all brokers from the current Config.
        """
        self.message_broker.PARAMETERS_CLASS = deepcopy(Config.PARAMETERS)  # type: ignore[misc]
        self.message_broker.ROUTING_KEY_CLASS = deepcopy(Config.ROUTING_KEY)  # type: ignore[misc]
        if self.args_bucket_broker is not None:
            self.args_bucket_broker.BUCKET_CLASS = deepcopy(Config.BUCKET)
        if self.results_bucket_broker is not None:
            self.results_bucket_broker.BUCKET_CLASS = deepcopy(Config.RESULT_BUCKET)
