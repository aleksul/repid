from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.connections.abc import ServerT


class ServerRegistry:
    def __init__(self) -> None:
        self._servers: dict[str, ServerT] = {}
        self._default_server: str | None = None

    def register_server(self, name: str, server: ServerT, *, is_default: bool = False) -> None:
        """
        Register a server instance.

        Args:
            name: Unique name for the server
            server: Server instance
            is_default: Whether this should be the default server
        """
        self._servers[name] = server

        if is_default or self._default_server is None:
            self._default_server = name

    def get_server(self, name: str | None = None) -> ServerT | None:
        """
        Get a server by name, or the default server if no name provided.

        Args:
            name: Server name, or None for default server

        Returns:
            Server instance or None if not found
        """
        if name is None:
            name = self._default_server

        return self._servers.get(name) if name else None

    def list_servers(self) -> dict[str, ServerT]:
        """List all registered servers.

        Returns:
            dict[str, ServerT]: A dictionary of server names and their instances
        """
        return self._servers.copy()

    def get_default_server(self) -> ServerT | None:
        """Get the default server.

        Returns:
            ServerT | None: The default server or None if no default server is set
        """
        return self.get_server()

    def set_default_server(self, name: str) -> None:
        """Set the default server by name.

        Args:
            name: Name of the server to set as default

        Raises:
            ValueError: If the server is not found
        """
        if name not in self._servers:
            raise ValueError(f"Server '{name}' not found.")
        self._default_server = name

    @property
    def default(self) -> ServerT:
        """Get the default server.

        Returns:
            ServerT: The default server

        Raises:
            ValueError: If no default server is set
        """
        default_server = self.get_default_server()
        if default_server is None:
            raise ValueError("No default server is set.")
        return default_server

    def __getitem__(self, name: str) -> ServerT:
        return self._servers[name]
