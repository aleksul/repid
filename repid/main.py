from __future__ import annotations

import logging

from repid.connection import Connection
from repid.middlewares import Middleware

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Repid:
    __default_connection: Connection | None = None

    def __init__(
        self,
        dsn: str,
        dsn_args: str | None = None,
        dsn_result: str | None = None,
    ):
        self._conn = Connection(
            messager=dsn,
            args_bucketer=dsn_args,
            results_bucketer=dsn_result,
        )
        self.__class__.__default_connection = self._conn
        logging.info("Default connection set.")
        self.middleware = Middleware

    @classmethod
    def get_default_connection(cls) -> Connection:
        conn = cls.__default_connection
        if conn is None:
            raise ConnectionError("Default connection is not specified.")
        return conn
