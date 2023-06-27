from __future__ import annotations

import importlib.metadata
import importlib.util
import operator
from functools import lru_cache

from packaging.version import parse as parse_version

_operators = {
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
}


@lru_cache
def is_installed(dependency: str, version_constraints: str | None = None) -> bool:
    spec = importlib.util.find_spec(dependency)
    if spec is None:
        return False

    if version_constraints is None:
        return True
    constraints = version_constraints.split(",")

    if not all(c.startswith((">", "<", "==", ">=", "<=")) for c in constraints):
        raise ValueError("Version constraint must contain an operator.")

    installed_version = parse_version(importlib.metadata.version(dependency))

    # validate every constraint
    for constraint in constraints:
        op: str
        if constraint.startswith((">", "<")) and not constraint.startswith((">=", "<=")):
            op = constraint[0]
            c = constraint[1:]
        else:
            op = constraint[0:2]
            c = constraint[2:]

        if not _operators[op](installed_version, parse_version(c)):
            return False

    return True
