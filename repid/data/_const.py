import sys

SLOTS_DATACLASS = dict(slots=True) if sys.version_info >= (3, 10) else dict()
