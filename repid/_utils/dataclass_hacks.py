import sys

SLOTS_DATACLASS = {"slots": True} if sys.version_info >= (3, 10) else {}
FROZEN_DATACLASS = {"frozen": True}  # this is a hack for mypy
# for some reason directly declaring frozen dataclass makes mypy think that the dataclass isn't
# implementing the protocol
