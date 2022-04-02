from repid.connection import Bucketing, Messaging
from repid.middlewares import AVAILABLE_FUNCTIONS


def test_available_functions():
    for name in dir(Messaging) + dir(Bucketing):
        if name.startswith("__"):
            continue
        if callable(getattr(Messaging, name, None) or getattr(Bucketing, name, None)):
            assert name in AVAILABLE_FUNCTIONS
