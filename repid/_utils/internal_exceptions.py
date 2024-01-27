from __future__ import annotations


class _NoAction(BaseException):
    # inherit from BaseException, as it isn't intended to be catched when `except Exception` is used
    def __init__(
        self,
        *,
        success: bool,
        data: str | None,
        exception: Exception | None,
    ) -> None:
        self.success = success
        self.data = data
        self.exception = exception
        super().__init__()
