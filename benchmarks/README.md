# Benchmarks

## Preparation

Benchmarks are meant to be executed on Python 3.11.

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

Before running another benchmark you need to manually delete created queues in RabbitMQ.
You can use web UI for that at <http://localhost:15672>.
