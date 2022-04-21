from typing import TYPE_CHECKING, Literal, Optional, Protocol

if TYPE_CHECKING:
    from repid.data import AnyBucketT, AnyMessageT
    from repid.queue import Queue


class Messaging(Protocol):
    supports_delayed_messages: bool = False
    queue_type: Literal["FIFO", "LIFO", "SIMPLE"] = "FIFO"
    priorities_distribution: str

    async def consume(self, queue: Queue) -> Optional[AnyMessageT]:
        """Consumes one message from the specified queue."""
        ...

    async def enqueue(
        self,
        message: AnyMessageT,
        priority: Literal["HIGH", "NORMAL", "LOW"] = "NORMAL",
    ) -> None:
        """Appends the message to the queue."""
        ...

    async def queue_declare(self, queue: Queue) -> None:
        """Creates the specified queue."""
        ...

    async def queue_flush(self, queue: Queue) -> None:
        """Empties the queue. Doesn't delete the queue itself."""
        ...

    async def queue_delete(self, queue: Queue) -> None:
        """Deletes the queue with all of its messages."""
        ...

    async def message_ack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution succeed."""
        ...

    async def message_nack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution failed."""
        ...


class Bucketing(Protocol):
    async def get_bucket(self, id_: str) -> AnyBucketT:
        """Retrivies the bucket of the job."""
        ...

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        """Stores the bucket of the job."""
        ...

    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket of the job."""
        ...
