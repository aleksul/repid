# Benchmarks

## Preparation

Benchmarks are meant to be executed on Python 3.11+.

RabbitMQ 4.x is required (AMQP 1.0 is built-in from 4.0).

```bash
python -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

docker compose up -d
```

## Running benchmarks

Each benchmark has a set of constants after the import. Feel free to tweak those.

```bash
python benchmark_repid.py

python benchmark_arq.py

python benchmark_dramatiq.py

python benchmark_celery.py
```

`benchmark_repid.py` automatically purges its queue before each run via the
RabbitMQ Management API. For the other benchmarks, delete their queues manually
before switching between them. You can use the web UI at <http://localhost:15672>.
