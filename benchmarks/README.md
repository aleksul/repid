# Benchmarks

## Preparation

```bash
python3.10 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

docker compose up -d
```

## Running benchmarks

Each benchmark has a set of constants after the import. Feel free to tweak those.

```bash
python3.10 benchmark_repid.py

python3.10 benchmark_dramatiq.py

python3.10 benchmark_celery.py
```

Before running another benchmark you need to manually delete created queues in RabbitMQ.
You can use web UI for that at <http://localhost:15672>.
