import warnings

warnings.warn(
    "Module repid.connections.dummy was renamed to repid.connections.in_memory",
    category=DeprecationWarning,
    stacklevel=2,
)

from repid.connections.in_memory import DummyBucketBroker, DummyMessageBroker  # noqa: E402

__all__ = ["DummyMessageBroker", "DummyBucketBroker"]
