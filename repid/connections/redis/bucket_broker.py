from __future__ import annotations

from typing import TYPE_CHECKING

from redis.asyncio.client import Redis

from repid.connections.abc import BucketBrokerT
from repid.data._buckets import ArgsBucket, ResultBucket
from repid.logger import logger

if TYPE_CHECKING:
    from repid.data import BucketT


class RedisBucketBroker(BucketBrokerT):
    def __init__(self, dsn: str, *, use_result_bucket: bool = False):
        self.BUCKET_CLASS = ResultBucket if use_result_bucket else ArgsBucket
        self.conn: Redis[bytes] = Redis.from_url(dsn)

    async def connect(self) -> None:
        await self.conn.ping()

    async def disconnect(self) -> None:
        await self.conn.aclose(close_connection_pool=True)  # type: ignore[attr-defined]

    async def get_bucket(self, id_: str) -> BucketT | None:
        logger.debug("Getting bucket with id: {id_}.", extra={"id_": id_})
        data = await self.conn.get(id_)
        if data is not None:
            return self.BUCKET_CLASS.decode(data.decode())  # type: ignore[no-any-return]
        return None

    async def store_bucket(self, id_: str, payload: BucketT) -> None:
        logger.debug("Storing bucket with id: {id_}.", extra={"id_": id_})
        await self.conn.set(
            id_,
            payload.encode(),
            exat=payload.timestamp + payload.ttl if payload.ttl is not None else None,
        )

    async def delete_bucket(self, id_: str) -> None:
        logger.debug("Deleting bucket with id: {id_}.", extra={"id_": id_})
        await self.conn.delete(id_)
