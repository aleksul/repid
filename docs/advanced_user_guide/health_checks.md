# Health checks

Repid comes with builtin HTTP server to provide you ability to make health checks.

To enable it, simply set `run_health_check_server` to True when creating your Worker.

```python hl_lines="6"
from repid import Worker

# code above is omitted

myworker = Worker(
    run_health_check_server=True,
)

# code below is omitted
```

The health check server will then be started when you run your Worker.

```python
# health check server is not yet started

await myworker.run()  # health check server runs as long as Worker runs

# health check server is shut down
```

By default the server will be launched with address `0.0.0.0` on port `8080`,
default endpoint is `/healthz`. You can override those parameters with settings.

```python hl_lines="7-11"
from repid import Worker, HealthCheckServerSettings

# code above is omitted

myworker = Worker(
    run_health_check_server=True,
    health_check_server_settings=HealthCheckServerSettings(
        address="127.0.0.1",
        port="12345",
        endpoint_name="/health-check",
    ),
)

# code below is omitted
```
