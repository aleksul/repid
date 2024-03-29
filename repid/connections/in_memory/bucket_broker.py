from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from repid.connections.abc import BucketBrokerT
from repid.data._buckets import ArgsBucket, ResultBucket
from repid.logger import logger

if TYPE_CHECKING:
    from repid.data.protocols import BucketT


class InMemoryBucketBroker(BucketBrokerT):
    def __init__(self, *, use_result_bucket: bool = False) -> None:
        self.BUCKET_CLASS = ResultBucket if use_result_bucket else ArgsBucket
        self.__storage: dict[str, BucketT] = {}

    async def connect(self) -> None:
        logger.info("Connecting to in-memory bucket broker.")
        await asyncio.sleep(0)

    async def disconnect(self) -> None:
        logger.info("Disconnecting from in-memory bucket broker.")
        await asyncio.sleep(0)

    async def get_bucket(self, id_: str) -> BucketT | None:
        logger.debug("Getting bucket with id: {id_}.", extra={"id_": id_})
        await asyncio.sleep(0)
        return self.__storage.get(id_, None)

    async def store_bucket(self, id_: str, payload: BucketT) -> None:
        logger.debug("Storing bucket with id: {id_}.", extra={"id_": id_})
        await asyncio.sleep(0)
        self.__storage[id_] = payload
        await asyncio.sleep(0)

    async def delete_bucket(self, id_: str) -> None:
        logger.debug("Deleting bucket with id: {id_}.", extra={"id_": id_})
        await asyncio.sleep(0)
        self.__storage.pop(id_, None)
        await asyncio.sleep(0)


def DummyBucketBroker(*, use_result_bucket: bool = False) -> InMemoryBucketBroker:  # noqa: N802
    from warnings import warn

    warn(
        "DummyBucketBroker was renamed to InMemoryBucketBroker.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return InMemoryBucketBroker(use_result_bucket=use_result_bucket)
