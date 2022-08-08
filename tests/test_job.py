from datetime import datetime, timedelta

import pytest

from repid import Connection, Job, Queue


async def test_not_deferred_job(fake_connection: Connection):
    j = Job("awesome_job")
    assert not j.is_deferred


@pytest.mark.parametrize(
    "deferred_until,deferred_by,cron",
    [
        (None, timedelta(minutes=1), None),
        (None, 60, None),
        (1659603596, None, None),
        (1659603596, 60, None),
        (None, None, "5 4 * * *"),
        (1659603596, None, "5 4 * * *"),
        (datetime(year=2020, month=1, day=1), None, None),
        (datetime(year=2020, month=1, day=1), timedelta(days=1), None),
        (datetime(year=2020, month=1, day=1), None, "5 4 * * *"),
    ],
)
async def test_deferred_job_ok(deferred_until, deferred_by, cron, fake_connection: Connection):
    j = Job("awesome_job", deferred_until=deferred_until, deferred_by=deferred_by, cron=cron)
    assert j.is_deferred


@pytest.mark.parametrize(
    "deferred_until,deferred_by,cron",
    [
        (None, 60, "5 4 * * *"),
        (None, timedelta(minutes=1), "5 4 * * *"),
        (datetime(year=2020, month=1, day=1), timedelta(days=1), "5 4 * * *"),
    ],
)
async def test_deferred_job_fail(deferred_until, deferred_by, cron, fake_connection: Connection):
    with pytest.raises(ValueError):
        Job("awesome_job", deferred_until=deferred_until, deferred_by=deferred_by, cron=cron)


async def test_wrong_equals(fake_connection: Connection):
    j = Job("awesome_job")
    assert not (j == 123)


async def test_inappropriate_job_name(fake_connection: Connection):
    with pytest.raises(ValueError, match="Job name must"):
        Job("some!@#$%^inappropriate_name")


async def test_non_default_queue(fake_connection: Connection):
    Job("awesome_job", queue="nondefault")
    Job("second_job", queue=Queue("nondefault"))
    with pytest.raises(ValueError, match="Queue name must"):
        Job("third_job", queue="some!@#$%^inappropriate_name")


async def test_job_str(fake_connection: Connection):
    j = Job("awesome_job")
    assert str(j) == "Job(name=awesome_job, queue=default)"
