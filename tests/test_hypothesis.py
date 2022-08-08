from datetime import datetime, timedelta

from hypothesis import given
from hypothesis.strategies import (
    builds,
    datetimes,
    from_regex,
    integers,
    none,
    one_of,
    sampled_from,
    timedeltas,
)

from repid.data import PrioritiesT
from repid.job import Job
from repid.queue import Queue
from repid.utils import VALID_ID, VALID_NAME


def queue_st():
    return builds(
        Queue,
        name=from_regex(VALID_NAME, fullmatch=True),
        _connection=none(),
    )


def job_st():
    return builds(
        Job,
        name=from_regex(VALID_NAME, fullmatch=True),
        queue=one_of(
            from_regex(VALID_NAME, fullmatch=True),
            queue_st(),
        ),
        priority=one_of(sampled_from(PrioritiesT), none()),
        deferred_until=one_of(
            datetimes(min_value=datetime(1970, 1, 1)),
            integers(min_value=0),
            none(),
        ),
        deferred_by=one_of(
            timedeltas(min_value=timedelta(seconds=0)),
            integers(min_value=0),
            none(),
        ),
        retries=integers(min_value=1),
        timeout=integers(min_value=1),
        ttl=one_of(integers(min_value=1), none()),
        id_=one_of(from_regex(VALID_ID, fullmatch=True), none()),
        _connection=none(),
    )


@given(job=job_st())
def test_job_creation(fake_connection, job):
    assert job
