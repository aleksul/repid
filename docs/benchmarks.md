# Benchmarks

???+ Note "Disclaimer"
    The benchmarks documented in this section are intended to provide information on the performance
    of the software being tested under specific conditions.
    Never use benchmarks as the one and only decision maker.

    All results were conducted on the same machine, using 8 cores/16 threads processor.
    Top performers (Repid and Dramatiq with gevent) used up to 100% CPU resources, so it's likely
    that they can perform even better with higher end CPU. Obviously, absolute results will be
    different on different systems.

    You should always try to replicate benchmark results in your environment. For that purpose,
    please find `benchmarks` directory in the Repid repository.

    Finally, it is important to note that benchmarking is a complex process, results of which can be
    affected by various factors. Therefore, I encourage readers to write their own benchmarks for
    their exact purposes and run them in the appropriate environment.

## Considerations

The benchmark tests rate of processing I/O limited tasks.
The task sleeps for 1 second (imitating I/O bound behavior, e.g. a very long running DB call)
and then increments a Redis field, which is also used to monitor amount of processed tasks.

Every library has been benchmarked 3 times and the results have been averaged.

Due to very huge difference in speed between tested libraries, amount of messages was adjusted so
that processing takes > 10 seconds & < 3 minutes.
Practically only values 800, 8000, 80000 were used.

For celery & dramatiq both "green threads" (eventlet & gevent respectively) & normal variants have
been tested. One has to keep in mind that green threads in Python are doing monkey-patching,
so one can face all sorts of weird behavior.

Repid and arq are the only 2 libraries, which are able to take advantage of Python's asyncio.

Every library uses RabbitMQ as a message broker, except arq, which only supports Redis.

Repid uses [uvloop](https://github.com/MagicStack/uvloop) to gain a bit more performance.
I have tried using uvloop with arq as well, but faced inconsistent behavior,
so I ended up disabling it.

### Credits

Links to other tested libraries:

- [dramatiq](https://github.com/Bogdanp/dramatiq)
- [celery](https://github.com/celery/celery)
- [arq](https://github.com/samuelcolvin/arq)

## The charts

Rates with green threads enabled for celery and dramatiq, in messages processed per second:

![Benchmark chart with green threads](benchmarks_img/chart-green-threads.svg)

Rates without green threads, in messages processed per second:

![Benchmark chart without green threads](benchmarks_img/chart-normal.svg)
