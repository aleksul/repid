import logging
from typing import Union

from redis.asyncio.client import Redis

from repid.data import AnyBucketT
from repid.middlewares import InjectMiddleware
from repid.serializer import BucketSerializer

logger = logging.getLogger(__name__)


@InjectMiddleware
class RedisBucketing:
    def __init__(self, dsn: str):
        self.conn = Redis.from_url(dsn)

    async def get_bucket(self, id_: str) -> Union[AnyBucketT, None]:
        logger.debug(f"Getting bucket with {id_ = }.")
        data = await self.conn.get(id_)
        if data is not None:
            return BucketSerializer.decode(data)
        return None

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        logger.debug(f"Storing {bucket = }.")
        await self.conn.set(
            bucket.id_,
            BucketSerializer.encode(bucket),
            exat=bucket.timestamp + bucket.ttl if bucket.ttl is not None else None,
        )

    async def delete_bucket(self, id_: str) -> None:
        logger.debug(f"Deleting bucket with {id_ = }.")
        await self.conn.delete(id_)
