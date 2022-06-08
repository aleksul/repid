from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from repid.connections.connection import Connection

DEFAULT_CONNECTION: Optional["Connection"] = ContextVar("DEFAULT_CONNECTION", default=None)
