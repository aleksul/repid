from datetime import datetime, timedelta
from functools import partial
from string import printable

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import (
    booleans,
    builds,
    datetimes,
    dictionaries,
    floats,
    from_regex,
    integers,
    just,
    lists,
    none,
    one_of,
    recursive,
    sampled_from,
    text,
    timedeltas,
)

from repid import Job, PrioritiesT, Queue, default_retry_policy_factory
from repid._utils import VALID_ID, VALID_NAME

MAX = 2**31 - 1
MIN = -(2**31)

json_st = recursive(
    none() | booleans() | floats() | text(printable),
    lambda children: lists(children) | dictionaries(text(printable), children),
)


queue_st = builds(
    Queue,
    name=from_regex(VALID_NAME, fullmatch=True),
    _connection=none(),
)


unfinished_job_st = partial(
    builds,
    Job,
    name=from_regex(VALID_NAME, fullmatch=True),
    queue=one_of(from_regex(VALID_NAME, fullmatch=True), queue_st),
    priority=sampled_from(PrioritiesT),
    id_=one_of(from_regex(VALID_ID, fullmatch=True), none()),
    deferred_until=one_of(datetimes(), none()),
    retries=integers(min_value=0, max_value=MAX),
    timeout=timedeltas(min_value=timedelta(seconds=1)),
    ttl=one_of(timedeltas(min_value=timedelta(seconds=1)), none()),
    args_id=one_of(from_regex(VALID_ID, fullmatch=True), none()),
    args_ttl=one_of(timedeltas(min_value=timedelta(seconds=1)), none()),
    args=one_of(none(), json_st),
    use_args_bucketer=one_of(none(), booleans()),
    result_id=one_of(from_regex(VALID_ID, fullmatch=True), none()),
    result_ttl=one_of(timedeltas(min_value=timedelta(seconds=1)), none()),
    store_result=one_of(none(), booleans()),
    _connection=none(),
)

deferred_by_job_st = unfinished_job_st(
    deferred_by=one_of(
        timedeltas(
            min_value=timedelta(seconds=1),
            max_value=datetime.max - datetime.now(),
        ),
        none(),
    ),
    cron=none(),
)

cron_job_st = unfinished_job_st(
    deferred_by=none(),
    cron=one_of(
        just("5 4 * * *"),
        just("0 22 * * 1-5"),
        just("23 0-20/2 * * *"),
        none(),
    ),
)

job_st = one_of(deferred_by_job_st, cron_job_st)


@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
@given(queue=queue_st)
@pytest.mark.usefixtures("fake_connection")
async def test_queue_methods(queue: Queue) -> None:
    assert queue
    await queue.declare()
    await queue.flush()
    await queue.delete()


@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    max_examples=1000,
    deadline=None,
)
@given(job=job_st)
@pytest.mark.usefixtures("fake_connection")
async def test_job_creation(job: Job) -> None:
    assert job
    await job.queue.declare()
    await job.enqueue()


@given(retry_number=integers(min_value=1))
def test_retry_policy(retry_number: int) -> None:
    policy = default_retry_policy_factory()
    policy(retry_number)
