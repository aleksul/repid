import json
from dataclasses import asdict

import pytest

from repid.models import JobDefenition


@pytest.mark.asyncio()
async def test_dict_to_json():
    j1 = JobDefenition(name="myjob")
    dumped = json.dumps(asdict(j1))
    j2 = JobDefenition(**json.loads(dumped))
    assert j1 == j2
