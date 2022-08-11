import logging
from contextvars import ContextVar
from typing import Union

from repid.connection import Connection
from repid.connections import _get_bucketing_from_string, _get_messaging_from_string
from repid.middlewares import Middleware

logging.getLogger(__name__).addHandler(logging.NullHandler())

DEFAULT_CONNECTION: ContextVar[Connection] = ContextVar("DEFAULT_CONNECTION")


class Repid:
    def __init__(
        self,
        dsn: str,
        dsn_args: Union[str, None] = None,
        dsn_result: Union[str, None] = None,
    ):
        global DEFAULT_CONNECTION

        self.__conn = Connection(
            messager=_get_messaging_from_string(dsn),
            args_bucketer=None if dsn_args is None else _get_bucketing_from_string(dsn_args),
            results_bucketer=None if dsn_result is None else _get_bucketing_from_string(dsn_result),
        )
        DEFAULT_CONNECTION.set(self.__conn)
        self.middleware = Middleware()
