from __future__ import annotations


class ChannelOverride:
    """Configuration overrides for a specific channel."""

    def __init__(
        self,
        *,
        project: str | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> None:
        self._data: dict[str, str] = {}
        if project is not None:
            self._data["project"] = project
        if topic is not None:
            self._data["topic"] = topic
        if subscription is not None:
            self._data["subscription"] = subscription

    def __getitem__(self, key: str) -> str:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._data.get(key, default)
