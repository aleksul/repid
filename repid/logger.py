from __future__ import annotations

import logging
from typing import Any, MutableMapping


class FormatWithExtraAdapter(logging.LoggerAdapter):
    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        if extra := kwargs.get("extra", None):
            msg = msg.format(**extra)
        return msg, kwargs


logger = logging.getLogger("repid")
logger.addHandler(logging.NullHandler())
logger = FormatWithExtraAdapter(logger, dict())  # type: ignore[assignment]
