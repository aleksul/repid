from uuid import uuid4

from repid import Repid
from repid.data._buckets import ArgsBucket, ResultBucket


async def test_args_bucket_flow(autoconn_with_buckets: Repid) -> None:
    async with autoconn_with_buckets.magic() as conn:
        bb = conn.args_bucket_broker
        assert bb is not None

        bucket_id = uuid4().hex

        no_bucket = await bb.get_bucket(bucket_id)
        assert no_bucket is None

        bucket = ArgsBucket(data="1234567890")
        await bb.store_bucket(bucket_id, bucket)

        assert await bb.get_bucket(bucket_id) == bucket

        await bb.delete_bucket(bucket_id)

        no_bucket = await bb.get_bucket(bucket_id)
        assert no_bucket is None


async def test_result_bucket_flow(autoconn_with_buckets: Repid) -> None:
    async with autoconn_with_buckets.magic() as conn:
        bb = conn.results_bucket_broker
        assert bb is not None

        bucket_id = uuid4().hex

        no_bucket = await bb.get_bucket(bucket_id)
        assert no_bucket is None

        bucket = ResultBucket(data="1234567890", started_when=111, finished_when=111)
        await bb.store_bucket(bucket_id, bucket)

        assert await bb.get_bucket(bucket_id) == bucket

        await bb.delete_bucket(bucket_id)

        no_bucket = await bb.get_bucket(bucket_id)
        assert no_bucket is None
