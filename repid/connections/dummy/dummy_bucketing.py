import logging
from typing import Dict, Union

from repid.data import AnyBucketT
from repid.middlewares import InjectMiddleware
from repid.serializer import BucketSerializer

logger = logging.getLogger(__name__)


@InjectMiddleware
class DummyBucketing:
    def __init__(self, dsn: str) -> None:
        self.buckets: Dict[str, bytes] = dict()

    async def get_bucket(self, id_: str) -> Union[AnyBucketT, None]:
        logger.debug(f"Getting bucket with id = '{id_}'.")
        value = self.buckets.get(id_)
        if value is not None:
            return BucketSerializer.decode(value)
        return None

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        logger.debug(f"Storing bucket with id = '{bucket.id_}'.")
        self.buckets[bucket.id_] = BucketSerializer.encode(bucket)

    async def delete_bucket(self, id_: str) -> None:
        logger.debug(f"Deleting bucket with id = '{id_}'.")
        self.buckets.pop(id_, None)
