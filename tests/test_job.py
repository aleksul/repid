from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest

from repid import Connection, Job, Queue

pytestmark = pytest.mark.usefixtures("fake_connection")


def test_not_deferred_job() -> None:
    j = Job("awesome_job")
    assert not j.is_deferred


@pytest.mark.parametrize(
    ("deferred_until", "deferred_by", "cron"),
    [
        (None, timedelta(minutes=1), None),
        (None, None, "5 4 * * *"),
        (datetime(year=2020, month=1, day=1), None, None),
        (datetime(year=2020, month=1, day=1), timedelta(days=1), None),
        (datetime(year=2020, month=1, day=1), None, "5 4 * * *"),
    ],
)
def test_deferred_job_ok(
    deferred_until: datetime | None,
    deferred_by: timedelta | None,
    cron: str | None,
) -> None:
    j = Job("awesome_job", deferred_until=deferred_until, deferred_by=deferred_by, cron=cron)
    assert j.is_deferred


@pytest.mark.parametrize(
    ("deferred_until", "deferred_by", "cron"),
    [
        (None, 60, "5 4 * * *"),
        (None, timedelta(minutes=1), "5 4 * * *"),
        (datetime(year=2020, month=1, day=1), timedelta(days=1), "5 4 * * *"),
    ],
)
def test_deferred_job_fail(
    deferred_until: datetime | None,
    deferred_by: timedelta | None,
    cron: str | None,
) -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        Job("awesome_job", deferred_until=deferred_until, deferred_by=deferred_by, cron=cron)


def test_inappropriate_job_name() -> None:
    with pytest.raises(ValueError, match="Job name must"):
        Job("some!@#$%^inappropriate_name")


def test_non_default_queue() -> None:
    Job("awesome_job", queue="nondefault")
    Job("second_job", queue=Queue("nondefault"))
    with pytest.raises(ValueError, match="Queue name must"):
        Job("third_job", queue="some!@#$%^inappropriate_name")


def test_job_uniqueness() -> None:
    j = Job("awesome_job")
    assert j.is_unique is False

    j = Job("second_job", id_="someid")
    assert j.is_unique is True


def test_job_overdue() -> None:
    j = Job("no_ttl_job")
    assert j.is_overdue is False

    j = Job("awesome_job", ttl=timedelta(seconds=1))
    assert j.is_overdue is False
    time.sleep(1.0)
    assert j.is_overdue is True


async def test_args_bucket_in_result_bucket_broker(fake_connection: Connection) -> None:
    j = Job("awesome_job")

    await fake_connection._rb.store_bucket(j.result_id, fake_connection._ab.BUCKET_CLASS(data=""))

    assert await j.result is None


async def test_incorrect() -> None:
    with pytest.raises(ValueError, match="Job id must contain only"):
        Job("awesome_job", id_="&*#^(jdbfsiduf878")

    with pytest.raises(ValueError, match="Deferred_by must be greater than"):
        Job("awesome_job", deferred_by=timedelta(seconds=-1))

    with pytest.raises(ValueError, match="Retries must be greater than"):
        Job("awesome_job", retries=-1)

    with pytest.raises(ValueError, match="Execution timeout must be greater than"):
        Job("awesome_job", timeout=timedelta(seconds=-1))

    with pytest.raises(ValueError, match="TTL must be greater than"):
        Job("awesome_job", ttl=timedelta(seconds=-1))

    with pytest.raises(ValueError, match="Args TTL must be greater than"):
        Job("awesome_job", args_ttl=timedelta(seconds=-1))

    with pytest.raises(ValueError, match="Result id must contain only"):
        Job("awesome_job", result_id="&*#^(jdbfsiduf878")

    with pytest.raises(ValueError, match="Result TTL must be greater than"):
        Job("awesome_job", result_ttl=timedelta(seconds=-1))
