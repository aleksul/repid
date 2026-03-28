# Built-in Servers

The `run_worker` function is the entrypoint for spinning up Repid's built-in sub-servers. These
servers run as background tasks alongside the main consumer loop, sharing its lifecycle.

!!! warning "Internal Infrastructure Only"
    These built-in servers are extremely lightweight and
    simple by design. They do not possess the security, performance,
    or robust routing features of a dedicated web framework like
    FastAPI or Litestar. **As a best practice, avoid exposing these built-
    in servers directly to the public internet.**
    They should be used strictly for internal infrastructure
    (like Kubernetes health probes) or local development.

## AsyncAPI Server

If you want to serve an interactive documentation UI alongside your worker on a specific port, pass
an `asyncapi_server=AsyncAPIServerSettings(...)` object. [Read more
here.](../../integrations/asyncapi.md)

## Health Check Server

In containerized environments like Kubernetes, the orchestrator needs a way to know if your worker
is alive, responsive, and hasn't silently deadlocked.

Repid comes with a built-in lightweight HTTP server designed specifically for liveness/readiness
probes.

To enable it, pass a `HealthCheckServerSettings` configuration to `run_worker`.

```python
from repid import Repid, HealthCheckServerSettings

app = Repid()

... # broker configuration is omitted

await app.run_worker(
    health_check_server=HealthCheckServerSettings(
        address="0.0.0.0",
        port=8080,
        endpoint_name="/healthz",
    )
)
```

With this configuration, your worker will spin up a small HTTP server in the background. Kubernetes
can then hit `http://<pod-ip>:8080/healthz`. As long as the Repid worker loop is successfully
iterating and the server connection is alive, the endpoint will return an HTTP 200 OK.
