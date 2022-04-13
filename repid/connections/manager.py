from typing import Dict, Optional

# from repid.connections.connection import Connection


# class ConnectionManager:
#     def __new__(cls):
#         if not hasattr(cls, "__instance"):
#             cls.__instance = super(ConnectionManager, cls).__new__(cls)
#         return cls.__instance

#     def __init__(self):
#         self.connections: Dict[str, Connection] = dict()
#         self._default_connection_name: str = "default"

#     @property
#     def default_connection(self) -> Connection:
#         if (conn := self.connections.get(self._default_connection_name)) is None:
#             raise ValueError("No default connection found.")
#         return conn

#     @default_connection.setter
#     def default_connection(self, connection_name: str) -> None:
#         self._default_connection_name = connection_name

#     def add_connection(self, connection: Connection, connection_name: Optional[str] = None) -> None:
#         if connection_name is None:
#             connection_name = self._default_connection_name
#         self.connections[connection_name] = connection

#     def get_connection(self, connection_name: Optional[str] = None) -> Optional[Connection]:
#         if connection_name is None:
#             connection_name = self._default_connection_name
#         return self.connections.get(connection_name)

#     def remove_connection(self, connection_name: Optional[str] = None) -> None:
#         if connection_name is None:
#             connection_name = self._default_connection_name
#         self.connections.pop(connection_name)
