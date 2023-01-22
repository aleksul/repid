# Deferred scheduling

You can create a job with delayed and recurring execution.
Let's focus on the options in the next sections.

## Delayed execution

To delay the execution of a Job, specify `deferred_until` argument.

```python hl_lines="7"
from datetime import datetime

from repid import Job

# code above is omitted

Job("delayed_job", deferred_until=datetime(2077, 1, 1))  # (1)

# code below is omitted
```

1. The job will be executed on the 1st of January in 2077.

## Recurring execution

!!! warning
    Next execution time for a recurring job is set after the previous execution. Therefore,
    if a job wasn't processed or processing took longer than recurring time frame, next execution
    will be set to the next earliest time.

### Defer by

If you want a job to be executed every equal period of time (e.g. every 10 minutes,
every 2 days, etc.) you can specify `deferred_by` argument.

```python hl_lines="7"
from datetime import timedelta

from repid import Job

# code above is omitted

Job("every_2_days", deferred_by=timedelta(days=2))

# code below is omitted
```

### cron

If you want to specify a job with recurring execution in [cron](https://wikipedia.org/wiki/Cron)
format, you will have to install repid with additional flag as follows:

```shell
pip install repid[cron]
```

It will install [croniter](https://github.com/kiorky/croniter) package, which will be used
internally to calculate next time of an execution.

You can specify any cron string supported by croniter using `cron` argument of a Job.

```python hl_lines="5"
from repid import Job

# code above is omitted

Job("every_day_at_noon", cron="0 12 * * *")

# code below is omitted
```

### Rescheduling and retries

Rescheduling for a retry will take precedence over rescheduling for the next recurring iteration.

Keep in mind, that depending on the way how your retries are set up, some recurring iterations might
be skipped.

If the number of retries was exceeded the job will be rescheduled for the next recurring
iteration anyway.

### Combining

Combining `deferred_by` and `cron` in one job is prohibited.

Combining `deferred_until` with either `deferred_by` or `cron` will delay the first execution
until `deferred_until` and recurrently continue using `deferred_by` or `cron`.

## Time-to-live

You can narrow the time of message consumption window using `ttl` argument. It will ensure that
since latest scheduling hasn't passed any more time. If not, the message will be marked as
'not acknowledged' and put in a dead-letter queue.

```python hl_lines="7"
from datetime import timedelta

from repid import Job

# code above is omitted

Job("consume_me_faster_than_3_days", ttl=timedelta(days=3))

# code below is omitted
```

!!! note
    Every rescheduling of a Job resets time-to-live timer. Time of rescheduling will be considered
    the new starting point.

## Recap

1. Delay job's execution using `deferred_until`
2. Create a recurring job using `deferred_by` or `cron`
3. Set `ttl` to ensure fast enough consumption
