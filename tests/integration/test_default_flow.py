from datetime import timedelta

from repid import Job, Worker


async def test_simple_job():
    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(messages_limit=1)

    hit = False

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        hit = True

    await myworker.run()
    assert hit


async def test_deferred_by_job():
    j = Job("awesome_job", deferred_by=timedelta(seconds=1))
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(messages_limit=3)

    hit = 0

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        hit += 1

    await myworker.run()
    assert hit == 3
