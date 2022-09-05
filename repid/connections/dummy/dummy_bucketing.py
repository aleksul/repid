from typing import TYPE_CHECKING, Dict, Union

from repid.logger import logger
from repid.middlewares import InjectMiddleware
from repid.serializer import BucketSerializer

if TYPE_CHECKING:
    from repid.data import AnyBucketT


@InjectMiddleware
class DummyBucketing:
    def __init__(self, dsn: str) -> None:
        self.buckets: Dict[str, bytes] = dict()

    async def get_bucket(self, id_: str) -> Union[AnyBucketT, None]:
        logger.debug("Getting bucket with id: {id_}.", extra=dict(id_=id_))
        value = self.buckets.get(id_)
        if value is not None:
            return BucketSerializer.decode(value)
        return None

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        logger.debug("Storing bucket with id: {id_}.", extra=dict(id_=bucket.id_))
        self.buckets[bucket.id_] = BucketSerializer.encode(bucket)

    async def delete_bucket(self, id_: str) -> None:
        logger.debug("Deleting bucket with id: {id_}.", extra=dict(id_=id_))
        self.buckets.pop(id_, None)
