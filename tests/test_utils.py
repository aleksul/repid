from typing import Callable, Optional

import pytest

from repid.connections.in_memory.utils import wait_until as dummy_wait_until
from repid.connections.rabbitmq.utils import wait_until as rabbitmq_wait_until
from repid.connections.redis.utils import wait_timestamp as redis_wait_timestamp
from repid.data._parameters import Parameters
from repid.utils import is_installed


@pytest.mark.parametrize(
    "params",
    [None, Parameters(delay=None)],  # type: ignore[arg-type]
)
@pytest.mark.parametrize(
    "function",
    [dummy_wait_until, rabbitmq_wait_until, redis_wait_timestamp],
)
def test_wait_until_returns_none(params: Optional[Parameters], function: Callable) -> None:
    assert function(params=params) is None


@pytest.mark.parametrize(
    ("dependency", "result"),
    [
        ("repid", True),
        ("pydantic", True),
        ("pytest", True),
        ("flask", False),
        ("blabla", False),
    ],
)
def test_is_imported(dependency: str, result: bool) -> None:
    assert is_installed(dependency) is result


@pytest.mark.parametrize(
    ("dependency", "constraints", "result"),
    [
        ("repid", ">=1.0.0,<2.0.0", True),
        ("repid", "<1.0.0", False),
        ("pydantic", ">=2.0.0a1,<3.0.0", True),
        ("pydantic", ">=1.0.0", True),
        ("pydantic", ">=3.0.0", False),
        ("pytest", ">=0.0.0,<100.0.0", True),
        ("flask", ">0.0.0,<2.0.0", False),
        ("blabla", ">0.0.0,<2.0.0", False),
    ],
)
def test_is_imported_with_constraints(
    dependency: str,
    constraints: str,
    result: bool,
) -> None:
    assert is_installed(dependency, constraints) is result


@pytest.mark.parametrize(
    ("dependency", "constraints"),
    [
        ("repid", "1.0.0,<2.0"),
        ("repid", "0.0.0"),
    ],
)
def test_is_imported_with_incorrect_constraints(dependency: str, constraints: str) -> None:
    with pytest.raises(ValueError, match="Version constraint must contain an operator."):
        is_installed(dependency, constraints)
