from dataclasses import dataclass
from typing import Iterable, Literal, Optional, Protocol

from repid.data import AnyBucketT, AnyMessageT


class Messaging(Protocol):
    supports_delayed_messages: bool
    queue_type: Literal["FIFO", "LIFO", "SIMPLE"]
    priorities_distribution: str

    def __init__(self, dsn: str) -> None:
        ...

    async def consume(
        self, queue_name: str, topics: Optional[Iterable[str]]
    ) -> Optional[AnyMessageT]:
        """Consumes one message from the specified queue. If topics are specified,
        only messages with those topics are consumed."""

    async def enqueue(self, message: AnyMessageT) -> None:
        """Appends the message to the queue."""

    async def ack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution succeed."""

    async def nack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution failed."""

    async def requeue(self, message: AnyMessageT) -> None:
        """Requeues the message."""

    async def queue_declare(self, queue_name: str) -> None:
        """Creates the specified queue."""

    async def queue_flush(self, queue_name: str) -> None:
        """Empties the queue. Doesn't delete the queue itself."""

    async def queue_delete(self, queue_name: str) -> None:
        """Deletes the queue with all of its messages."""

    async def maintenance(self) -> None:
        """Performs maintenance tasks."""


class Bucketing(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def get_bucket(self, id_: str) -> Optional[AnyBucketT]:
        """Retrivies the bucket of the job."""

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        """Stores the bucket of the job."""

    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket of the job."""


@dataclass(frozen=True)
class Connection:
    messager: Messaging
    args_bucketer: Optional[Bucketing] = None
    results_bucketer: Optional[Bucketing] = None
