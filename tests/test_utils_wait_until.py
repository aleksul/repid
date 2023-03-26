from typing import Callable, Optional

import pytest

from repid.connections.dummy.utils import wait_until as dummy_wait_until
from repid.connections.rabbitmq.utils import wait_until as rabbitmq_wait_until
from repid.connections.redis.utils import wait_timestamp as redis_wait_timestamp
from repid.data._parameters import Parameters


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
