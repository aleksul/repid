from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from aiokafka.abc import ConsumerRebalanceListener
    from aiokafka.structs import OffsetAndMetadata, RecordMetadata, TopicPartition


class ConsumerRecordProtocol(Protocol):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: bytes | None
    value: bytes | None
    headers: Sequence[tuple[str, bytes | None]] | None
    checksum: int | None
    serialized_key_size: int
    serialized_value_size: int


class AIOKafkaConsumerProtocol(Protocol):
    def subscribe(
        self,
        topics: tuple[str, ...],
        *,
        listener: ConsumerRebalanceListener,
    ) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[ConsumerRecordProtocol]]: ...
    async def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None: ...
    def pause(self, *partitions: TopicPartition) -> None: ...
    def paused(self) -> set[TopicPartition]: ...
    def resume(self, *partitions: TopicPartition) -> None: ...
    def assignment(self) -> set[TopicPartition]: ...


class AIOKafkaProducerProtocol(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send_and_wait(  # noqa: PLR0917
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> RecordMetadata | None: ...
