from repid.connections.in_memory import InMemoryServer
from repid.server_registry import ServerRegistry


def test_register_server() -> None:
    registry = ServerRegistry()
    server = InMemoryServer()

    registry.register_server("test", server)

    assert registry.get_server("test") is server


def test_register_default_server() -> None:
    registry = ServerRegistry()
    server = InMemoryServer()

    registry.register_server("test", server, is_default=True)

    assert registry.get_server() is server
    assert registry.get_server("test") is server


def test_get_nonexistent_server() -> None:
    registry = ServerRegistry()

    assert registry.get_server("nonexistent") is None


def test_get_server_without_default() -> None:
    registry = ServerRegistry()
    server = InMemoryServer()

    registry.register_server("test", server, is_default=False)

    # When no default is explicitly set, the first server becomes default
    assert registry.get_server() is server


def test_get_server_implicit_default() -> None:
    registry = ServerRegistry()
    server1 = InMemoryServer()
    server2 = InMemoryServer()

    registry.register_server("server1", server1)
    registry.register_server("server2", server2)

    # The first registered server should be the default
    assert registry.get_server() is server1


def test_multiple_servers() -> None:
    registry = ServerRegistry()
    server1 = InMemoryServer()
    server2 = InMemoryServer()

    registry.register_server("server1", server1, is_default=True)
    registry.register_server("server2", server2)

    assert registry.get_server("server1") is server1
    assert registry.get_server("server2") is server2
    assert registry.get_server() is server1


def test_override_default_server() -> None:
    registry = ServerRegistry()
    server1 = InMemoryServer()
    server2 = InMemoryServer()

    registry.register_server("server1", server1, is_default=True)
    registry.register_server("server2", server2, is_default=True)

    # The second registration should override the default
    assert registry.get_server() is server2
