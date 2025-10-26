from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import pytest

from repid._utils import JSON_ENCODER
from repid.data import BucketT, ResultBucketT
from repid.data._buckets import ArgsBucket, ResultBucket
from repid.data._key import RoutingKey
from repid.data._parameters import (
    DelayProperties,
    Parameters,
    ResultProperties,
    RetriesProperties,
)
from repid.data.priorities import PrioritiesT


def test_routing_key() -> None:
    # Test valid values
    routing_key = RoutingKey(
        topic="test_topic",
        queue="test_queue",
        priority=PrioritiesT.HIGH.value,
    )
    assert routing_key.topic == "test_topic"
    assert routing_key.queue == "test_queue"
    assert routing_key.priority == PrioritiesT.HIGH.value
    assert isinstance(UUID(routing_key.id_), UUID)

    # Test default values
    routing_key = RoutingKey(topic="test_topic")
    assert routing_key.topic == "test_topic"
    assert routing_key.queue == "default"
    assert routing_key.priority == PrioritiesT.MEDIUM.value
    assert isinstance(UUID(routing_key.id_), UUID)

    # Test invalid id
    with pytest.raises(ValueError, match="Incorrect id."):
        RoutingKey(topic="test_topic", id_="inva$*(lid_id")

    # Test invalid topic
    with pytest.raises(ValueError, match="Incorrect topic."):
        RoutingKey(topic="invalid topic")

    # Test invalid queue name
    with pytest.raises(ValueError, match="Incorrect queue name."):
        RoutingKey(topic="test_topic", queue="invalid queue name")

    # Test invalid priority
    with pytest.raises(ValueError, match="Invalid priority."):
        RoutingKey(topic="test_topic", priority=-1)


def test_retries_properties() -> None:
    # Test default initialization
    retries = RetriesProperties()
    assert retries.max_amount == 0
    assert retries.already_tried == 0

    # Test initialization with custom values
    retries = RetriesProperties(max_amount=3, already_tried=1)
    assert retries.max_amount == 3
    assert retries.already_tried == 1

    # Test encoding and decoding
    data = retries.encode()
    assert isinstance(data, str)

    decoded = RetriesProperties.decode(data)
    assert isinstance(decoded, RetriesProperties)
    assert decoded.max_amount == 3
    assert decoded.already_tried == 1


def test_result_properties() -> None:
    # Test default initialization
    result = ResultProperties()
    assert isinstance(result.id_, str)
    assert result.ttl is None

    # Test initialization with custom values
    result = ResultProperties(id_="foo", ttl=timedelta(seconds=60))
    assert result.id_ == "foo"
    assert result.ttl == timedelta(seconds=60)

    # Test encoding and decoding
    data = result.encode()
    assert isinstance(data, str)

    decoded = ResultProperties.decode(data)
    assert isinstance(decoded, ResultProperties)
    assert decoded.id_ == "foo"
    assert decoded.ttl == timedelta(seconds=60)


def test_delay_properties() -> None:
    # Test default initialization
    delay = DelayProperties()
    assert delay.delay_until is None
    assert delay.defer_by is None
    assert delay.cron is None
    assert delay.next_execution_time is None

    # Test initialization with custom values
    now = datetime.now()
    delay = DelayProperties(
        delay_until=now,
        defer_by=timedelta(seconds=60),
        cron="* * * * *",
        next_execution_time=now + timedelta(seconds=60),
    )
    assert delay.delay_until == now
    assert delay.defer_by == timedelta(seconds=60)
    assert delay.cron == "* * * * *"
    assert delay.next_execution_time == now + timedelta(seconds=60)

    # Test encoding and decoding
    data = delay.encode()
    assert isinstance(data, str)

    decoded = DelayProperties.decode(data)
    assert isinstance(decoded, DelayProperties)
    assert decoded.delay_until == now
    assert decoded.defer_by == timedelta(seconds=60)
    assert decoded.cron == "* * * * *"
    assert decoded.next_execution_time == now + timedelta(seconds=60)


def test_parameters() -> None:
    # Test default initialization
    params = Parameters()
    assert params.RETRIES_CLASS is RetriesProperties
    assert params.RESULT_CLASS is ResultProperties
    assert params.DELAY_CLASS is DelayProperties
    assert params.execution_timeout == timedelta(minutes=10)
    assert params.retries == RetriesProperties()
    assert params.result is None
    assert params.delay == DelayProperties()
    assert params.ttl is None
    assert isinstance(params.timestamp, datetime)

    # Test initialization with custom values
    retries = RetriesProperties(max_amount=3, already_tried=1)
    result = ResultProperties(id_="foo", ttl=timedelta(seconds=60))
    delay = DelayProperties(
        delay_until=datetime.now(),
        defer_by=timedelta(seconds=60),
        cron="* * * * *",
        next_execution_time=datetime.now() + timedelta(seconds=60),
    )
    execution_timeout = timedelta(minutes=1)
    ttl = timedelta(seconds=2)
    now = datetime.now()
    params = Parameters(
        execution_timeout=execution_timeout,
        retries=retries,
        result=result,
        delay=delay,
        ttl=ttl,
        timestamp=now,
    )
    assert params.execution_timeout == execution_timeout
    assert params.retries == retries
    assert params.result == result
    assert params.delay == delay
    assert params.ttl == ttl
    assert params.timestamp == now

    # Test encoding and decoding
    data = params.encode()
    assert isinstance(data, str)

    decoded = Parameters.decode(data)
    assert isinstance(decoded, Parameters)
    assert params.execution_timeout == execution_timeout
    assert decoded.retries == retries
    assert decoded.result == result
    assert decoded.delay == delay
    assert decoded.ttl == ttl
    assert decoded.timestamp == now


@pytest.mark.parametrize(
    ("ttl", "is_overdue"),
    [
        (timedelta(seconds=1), False),
        (None, False),
        (timedelta(seconds=-1), True),
    ],
)
def test_parameters_is_overdue(ttl: timedelta, is_overdue: bool) -> None:
    # Test that the is_overdue property works as expected
    assert Parameters(timestamp=datetime.now(), ttl=ttl).is_overdue == is_overdue


@pytest.mark.parametrize(
    ("bucket", "data", "timestamp", "ttl", "expected_encoded"),
    [
        (
            ArgsBucket,
            "some_data",
            datetime(2022, 1, 1),
            timedelta(days=1),
            '{"data":"some_data","timestamp":"2022-01-01T00:00:00","ttl":86400.0}',
        ),
        (
            ResultBucket,
            "some_data",
            datetime(2022, 1, 1),
            timedelta(days=1),
            '{"data":"some_data","started_when":1,"finished_when":2,'
            '"success":true,"exception":null,"timestamp":"2022-01-01T00:00:00","ttl":86400.0}',
        ),
    ],
)
def test_bucket_encode_decode(
    bucket: type[BucketT | ResultBucketT],
    data: str,
    timestamp: datetime,
    ttl: timedelta,
    expected_encoded: str,
) -> None:
    # Test that the encode and decode methods work as expected
    if bucket is ArgsBucket:
        bucket_instance = bucket(data=data, timestamp=timestamp, ttl=ttl)
    else:
        bucket_instance = bucket(  # type: ignore[call-arg]
            data=data,
            started_when=1,
            finished_when=2,
            timestamp=timestamp,
            ttl=ttl,
        )
    assert bucket_instance.encode() == expected_encoded
    assert bucket.decode(expected_encoded) == bucket_instance


@pytest.mark.parametrize(
    ("bucket", "ttl", "is_overdue"),
    [
        (ArgsBucket, timedelta(seconds=1), False),
        (ArgsBucket, None, False),
        (ArgsBucket, timedelta(seconds=-1), True),
        (ResultBucket, timedelta(seconds=1), False),
        (ResultBucket, None, False),
        (ResultBucket, timedelta(seconds=-1), True),
    ],
)
def test_bucket_is_overdue(
    bucket: type[BucketT | ResultBucketT],
    ttl: timedelta,
    is_overdue: bool,
) -> None:
    timestamp = datetime.now()
    # Test that the is_overdue property works as expected
    if bucket is ArgsBucket:
        bucket_instance = bucket(data="foo", timestamp=timestamp, ttl=ttl)
    else:
        bucket_instance = bucket(  # type: ignore[call-arg]
            data="foo",
            started_when=1,
            finished_when=2,
            timestamp=timestamp,
            ttl=ttl,
        )
    assert bucket_instance.is_overdue == is_overdue


@pytest.mark.parametrize("bucket", [ArgsBucket, ResultBucket])
def test_bucket_default_timestamp(bucket: type[BucketT | ResultBucketT]) -> None:
    # Test that the default timestamp value works as expected
    if bucket is ArgsBucket:
        bucket_instance = bucket(data="foo")
    else:
        bucket_instance = bucket(  # type: ignore[call-arg]
            data="foo",
            started_when=1,
            finished_when=2,
        )
    assert bucket_instance.timestamp <= datetime.now()


@pytest.mark.parametrize(
    ("bucket", "ttl"),
    [
        (ArgsBucket, None),
        (ResultBucket, None),
    ],
)
def test_bucket_none_ttl(bucket: type[BucketT | ResultBucketT], ttl: timedelta | None) -> None:
    # Test that the default ttl value is None
    if bucket == ArgsBucket:
        bucket_instance = bucket(data="foo", ttl=ttl)
    else:
        bucket_instance = bucket(  # type: ignore[call-arg]
            data="foo",
            started_when=1,
            finished_when=2,
            ttl=ttl,
        )
    assert bucket_instance.ttl is None


def test_resultbucket_exception() -> None:
    # Test that the exception property is encoded and decoded correctly

    now = datetime.now()
    exception = "Test exception"
    expected_encoded = (
        '{"data":"foo","started_when":1,"finished_when":2,'
        f'"success":false,"exception":"Test exception","timestamp":{JSON_ENCODER.encode(now)},'
        '"ttl":null}'
    )
    bucket = ResultBucket(
        data="foo",
        started_when=1,
        finished_when=2,
        success=False,
        exception=exception,
        timestamp=now,
    )
    assert bucket.encode() == expected_encoded
    assert ResultBucket.decode(expected_encoded) == bucket
