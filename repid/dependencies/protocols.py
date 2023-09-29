from typing import Protocol, runtime_checkable


@runtime_checkable
class DependencyT(Protocol):
    __repid_dependency__: bool = True
