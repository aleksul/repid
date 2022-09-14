import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

from repid.middlewares import AVAILABLE_FUNCTIONS, Middleware

if TYPE_CHECKING:
    from repid.connections import Bucketing, Messaging


@dataclass(frozen=True)
class Connection:
    messager: Messaging
    args_bucketer: Union[Bucketing, None] = None
    results_bucketer: Union[Bucketing, None] = None
    middleware: Middleware = field(default_factory=Middleware, init=False)

    def __post_init__(self) -> None:
        for proto in [self.messager, self.args_bucketer, self.results_bucketer]:
            if proto is None:
                continue
            for _, attr in inspect.getmembers(proto, predicate=inspect.ismethod):
                if (
                    callable(attr)
                    and attr.__name__ in AVAILABLE_FUNCTIONS
                    and hasattr(attr, "_repid_singal_emitter")
                ):
                    attr._repid_signal_emitter = self.middleware.emit_signal

    @property
    def _ab(self) -> Bucketing:  # args bucketer
        if self.args_bucketer is None:
            raise ValueError("Args bucketer is not configured.")
        return self.args_bucketer

    @property
    def _rb(self) -> Bucketing:  # results bucketer
        if self.results_bucketer is None:
            raise ValueError("Results bucketer is not configured.")
        return self.results_bucketer
