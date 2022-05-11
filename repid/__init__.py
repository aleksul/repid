from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from repid.connections.connection import Connection

_default_connection: Optional["Connection"] = None  # type: ignore[no-any-unimported]
