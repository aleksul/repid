import functools
import sys
from typing import Awaitable, Callable, TypeVar

import anyio

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")
T = TypeVar("T")


def asyncify(
    function: Callable[T_ParamSpec, T_Retval]  # type: ignore[misc]
) -> Callable[T_ParamSpec, Awaitable[T_Retval]]:  # type: ignore[misc]
    async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:  # type: ignore[name-defined]  # noqa: E501
        partial_f = functools.partial(function, *args, **kwargs)
        return await anyio.to_thread.run_sync(partial_f)

    return wrapper
