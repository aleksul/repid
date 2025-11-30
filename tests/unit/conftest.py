from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from repid import Repid
from repid.connections.in_memory import InMemoryServer


@pytest.fixture
def fake_repid() -> Repid:
    server = InMemoryServer()
    app = Repid()
    app.servers.register_server("default", server, is_default=True)
    return app


@pytest.fixture
async def fake_connection(fake_repid: Repid) -> AsyncIterator[InMemoryServer]:
    server = fake_repid.servers.get_server("default")
    assert server is not None
    assert isinstance(server, InMemoryServer)
    async with server.connection():
        yield server
