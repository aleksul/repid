import pytest


@pytest.fixture()
def anyio_backend(request):
    return ("asyncio", {"use_uvloop": True})
