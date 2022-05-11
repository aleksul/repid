from functools import partial
from typing import Any, Callable, Dict, Tuple

import anyio


def asyncify(function: Callable) -> Callable:
    async def wrapper(*args: Tuple, **kwargs: Dict) -> Any:
        return await anyio.to_thread.run_sync(partial(function, *args, **kwargs))

    return wrapper
