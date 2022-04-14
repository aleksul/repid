from typing import TYPE_CHECKING, AsyncGenerator, Literal, Protocol, Union

if TYPE_CHECKING:
    from repid.message import DeferredMessage, Message, Result
    from repid.queue import Queue

AnyMessage = Union[Message, DeferredMessage]


class Messaging(Protocol):
    supports_delayed_messages: bool = False
    queue_type: Literal["FIFO", "LIFO", "SIMPLE"] = "FIFO"

    async def consume(self, queue: Queue) -> AsyncGenerator[AnyMessage, None]:
        """Consumes messages from the specified queue."""
        ...

    async def enqueue(
        self,
        message: AnyMessage,
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

    async def message_ack(self, message: AnyMessage) -> None:
        """Informs message broker that job execution succeed."""
        ...

    async def message_nack(self, message: AnyMessage) -> None:
        """Informs message broker that job execution failed."""
        ...


class Resulting(Protocol):
    async def get_result(self, message_id: str) -> Result:
        """Retrivies the result of the job."""
        ...

    async def store_result(self, result: Result) -> None:
        """Stores the result of the job."""
        ...

    async def delete_result(self, message_id: str) -> None:
        """Deletes the result of the job."""
        ...
