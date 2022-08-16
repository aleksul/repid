from datetime import datetime, timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import (
    builds,
    characters,
    datetimes,
    dictionaries,
    floats,
    from_regex,
    integers,
    none,
    one_of,
    sampled_from,
    text,
    timedeltas,
    tuples,
)

from repid.data import Args, ArgsBucket, PrioritiesT
from repid.job import Job
from repid.queue import Queue
from repid.utils import VALID_ID, VALID_NAME

MAX = 2**31 - 1
MIN = -(2**31)


def queue_st():
    return builds(
        Queue,
        name=from_regex(VALID_NAME, fullmatch=True),
        _connection=none(),
    )


def jsonable():
    return one_of(
        integers(min_value=MIN, max_value=MAX),
        floats(min_value=MIN, max_value=MAX),
        none(),
        datetimes(),
        text(),
    )


def job_args_simple_st():
    return builds(
        Args,
        args=tuples(jsonable()),
        kwargs=dictionaries(keys=characters(), values=jsonable()),
    )


def job_args_bucket_st():
    return builds(
        ArgsBucket,
        id_=from_regex(VALID_ID, fullmatch=True),
        args=one_of(none(), tuples(jsonable())),
        kwargs=one_of(none(), dictionaries(keys=characters(), values=jsonable())),
        timestamp=integers(min_value=0, max_value=MAX),
        ttl=one_of(integers(min_value=1, max_value=MAX), none()),
    )


def job_st():
    return builds(
        Job,
        name=from_regex(VALID_NAME, fullmatch=True),
        queue=one_of(
            from_regex(VALID_NAME, fullmatch=True),
            queue_st(),
        ),
        priority=sampled_from(PrioritiesT),
        deferred_until=one_of(
            datetimes(min_value=datetime(1970, 1, 1)),
            integers(min_value=0, max_value=MAX),
            none(),
        ),
        deferred_by=one_of(
            timedeltas(min_value=timedelta(seconds=0)),
            integers(min_value=0, max_value=MAX),
            none(),
        ),
        retries=integers(min_value=1, max_value=MAX),
        timeout=integers(min_value=1, max_value=MAX),
        ttl=one_of(integers(min_value=1, max_value=MAX), none()),
        id_=one_of(from_regex(VALID_ID, fullmatch=True), none()),
        args=one_of(none(), job_args_simple_st(), job_args_bucket_st()),
        result_id=one_of(from_regex(VALID_ID, fullmatch=True), none()),
        result_ttl=one_of(integers(min_value=1, max_value=MAX), none()),
        _connection=none(),
    )


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(job=job_st())
async def test_job_creation(fake_connection, job):
    assert job
    await job.queue.declare()
    await job.enqueue()
