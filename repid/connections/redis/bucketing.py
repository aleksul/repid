from __future__ import annotations

from typing import TYPE_CHECKING

from redis.asyncio.client import Redis

from repid.logger import logger
from repid.middlewares import InjectMiddleware
from repid.serializer import BucketSerializer

if TYPE_CHECKING:
    from repid.data import AnyBucketT


@InjectMiddleware
class RedisBucketing:
    def __init__(self, dsn: str):
        self.conn = Redis.from_url(dsn)

    async def get_bucket(self, id_: str) -> AnyBucketT | None:
        logger.debug("Getting bucket with id: {id_}.", extra=dict(id_=id_))
        data = await self.conn.get(id_)
        if data is not None:
            return BucketSerializer.decode(data)
        return None

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        logger.debug("Storing bucket with id: {id_}.", extra=dict(id_=bucket.id_))
        await self.conn.set(
            bucket.id_,
            BucketSerializer.encode(bucket),
            exat=bucket.timestamp + bucket.ttl if bucket.ttl is not None else None,
        )

    async def delete_bucket(self, id_: str) -> None:
        logger.debug("Deleting bucket with id: {id_}.", extra=dict(id_=id_))
        await self.conn.delete(id_)
