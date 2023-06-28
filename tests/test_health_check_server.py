import httpx
import pytest

from repid import HealthCheckServer, HealthCheckServerSettings, HealthCheckStatus


async def test_default_health_check_server() -> None:
    h = HealthCheckServer()

    await h.start()

    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8080/healthz")
        assert response.status_code == 200

        h.health_status = HealthCheckStatus.UNHEALTHY

        response = await client.get("http://localhost:8080/healthz")
        assert response.status_code == 503

        h.health_status = HealthCheckStatus.OK

        response = await client.get("http://localhost:8080/healthz")
        assert response.status_code == 200

    await h.stop()


@pytest.mark.parametrize(
    ("port", "endpoint_name"),
    [
        (11111, "/health"),
        (10101, "/health-check"),
    ],
)
async def test_health_check_server(port: int, endpoint_name: str) -> None:
    h = HealthCheckServer(HealthCheckServerSettings(port=port, endpoint_name=endpoint_name))

    await h.start()

    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://localhost:{port}{endpoint_name}")
        assert response.status_code == 200

        h.health_status = HealthCheckStatus.UNHEALTHY

        response = await client.get(f"http://localhost:{port}{endpoint_name}")
        assert response.status_code == 503

        h.health_status = HealthCheckStatus.OK

        response = await client.get(f"http://localhost:{port}{endpoint_name}")
        assert response.status_code == 200

    await h.stop()


@pytest.mark.parametrize(
    ("port", "endpoint_name"),
    [
        (11111, "/health"),
        (10101, "/health-check"),
    ],
)
async def test_health_check_server_incorrect_endpoint(port: int, endpoint_name: str) -> None:
    h = HealthCheckServer(HealthCheckServerSettings(port=port, endpoint_name=endpoint_name))

    await h.start()

    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://localhost:{port}/wrong")
        assert response.status_code == 404

    await h.stop()
