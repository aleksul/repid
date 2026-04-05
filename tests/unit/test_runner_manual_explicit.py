from typing import Literal

import pytest

from repid import Repid, Router
from repid.test_client import TestClient

router = Router()


@router.actor(confirmation_mode="manual_explicit")
async def explicit_ack_actor() -> Literal["ack", "nack", "reject"]:
    return "ack"


@router.actor(confirmation_mode="manual_explicit")
async def explicit_nack_actor() -> Literal["ack", "nack", "reject"]:
    return "nack"


@router.actor(confirmation_mode="manual_explicit")
async def explicit_reject_actor() -> Literal["ack", "nack", "reject"]:
    return "reject"


@router.actor(confirmation_mode="manual_explicit")
async def explicit_no_action_actor() -> Literal["ack", "nack", "reject", "no_action"]:
    return "no_action"


@router.actor(confirmation_mode="manual_explicit")
async def explicit_invalid_actor() -> Literal["ack", "nack", "reject", "no_action"]:
    return "something else"  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_manual_explicit_ack() -> None:
    app = Repid()
    app.include_router(router)
    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            headers={"topic": "explicit_ack_actor"},
            payload=b"null",
        )
        assert client._processed_messages[0].action == "acked"


@pytest.mark.asyncio
async def test_manual_explicit_nack() -> None:
    app = Repid()
    app.include_router(router)
    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            headers={"topic": "explicit_nack_actor"},
            payload=b"null",
        )
        assert client._processed_messages[0].action == "nacked"


@pytest.mark.asyncio
async def test_manual_explicit_reject() -> None:
    app = Repid()
    app.include_router(router)
    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            headers={"topic": "explicit_reject_actor"},
            payload=b"null",
        )
        assert client._processed_messages[0].action == "rejected"


@pytest.mark.asyncio
async def test_manual_explicit_no_action() -> None:
    app = Repid()
    app.include_router(router)
    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            headers={"topic": "explicit_no_action_actor"},
            payload=b"null",
        )
        assert client._processed_messages[0].action is None


@pytest.mark.asyncio
async def test_manual_explicit_invalid() -> None:
    app = Repid()
    app.include_router(router)
    async with TestClient(app, raise_on_actor_error=True) as client:
        with pytest.raises(ValueError, match="Expected one of"):
            await client.send_message(
                channel="default",
                headers={"topic": "explicit_invalid_actor"},
                payload=b"null",
            )
