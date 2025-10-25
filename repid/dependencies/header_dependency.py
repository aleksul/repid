from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repid.dependencies._utils import DependencyContext


class Header:
    """Dependency annotation that indicates that the argument resolves to a header value.

    Args:
        name (str | None, optional): Override the name of the header. If None, default to the
        name of the argument. Defaults to None.
    """

    __slots__ = ("_name",)

    def __init__(self, *, name: str | None = None) -> None:
        self._name = name

    async def resolve(self, *, context: DependencyContext) -> Any:
        return context.parsed_headers[context.headers_id_to_name[id(self)]]
