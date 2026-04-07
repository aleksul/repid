from __future__ import annotations

from typing import Any, Protocol


class ConsumerRecordProtocol(Protocol):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]] | None
    checksum: int | None
    serialized_key_size: int
    serialized_value_size: int


class AIOKafkaConsumerProtocol(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[Any, list[ConsumerRecordProtocol]]: ...
    async def commit(self, offsets: dict[Any, Any] | None = None) -> None: ...
    def pause(self, *partitions: Any) -> None: ...
    def resume(self, *partitions: Any) -> None: ...
    def assignment(self) -> set[Any]: ...


class AIOKafkaProducerProtocol(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> Any: ...
