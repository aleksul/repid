from __future__ import annotations

import collections.abc
from dataclasses import dataclass
from typing import AsyncIterator, Callable, List, Optional, Union
from unittest.mock import MagicMock

import pytest

from repid.actor import ActorData
from repid.connection import Connection
from repid.connections import InMemoryBucketBroker, InMemoryMessageBroker
from repid.connections.in_memory.utils import DummyQueue
from repid.main import Repid
from repid.queue import Queue
from repid.router import Router
from repid.testing.modifiers import EventLog, EventLogModifier, RunWorkerOnEnqueueModifier
from repid.worker import Worker

GetEventLogT = Callable[[], List[EventLog]]
GetMockedActorT = Callable[[str], Optional[MagicMock]]
GetInMemoryQueueT = Callable[[Union[Queue, str]], Optional[DummyQueue]]


def pytest_configure(config: pytest.Config) -> None:
    # register repid marker
    config.addinivalue_line("markers", "repid: mark to enable repid testing plugin")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if (marker := item.get_closest_marker("repid")) is not None and (
            marker.kwargs.get("autoconnection") is not False
        ):
            item.fixturenames.append("repid_connection")  # type: ignore[attr-defined]

        for marker in item.iter_markers("repid"):
            if (declare := marker.kwargs.get("declare_all_known_queues")) is not None:
                if declare:
                    item.fixturenames.append("repid_declare_all_known_queues")  # type: ignore[attr-defined]
                break


@pytest.fixture()
def repid_app() -> Repid:
    return Repid(
        Connection(
            InMemoryMessageBroker(),
            InMemoryBucketBroker(),
            InMemoryBucketBroker(use_result_bucket=True),
        ),
    )


@pytest.fixture()
async def repid_connection(_repid_app_with_modifiers: Repid) -> AsyncIterator[Connection]:
    await _repid_app_with_modifiers.magic_disconnect()
    async with _repid_app_with_modifiers.magic(auto_disconnect=True):
        yield _repid_app_with_modifiers.connection


@pytest.fixture()
def repid_get_event_log(
    _repid_app_with_event_log_modifiers: EventLogModifiers,
) -> GetEventLogT:
    return lambda: sorted(
        (
            _repid_app_with_event_log_modifiers.message_broker.events
            + (
                _repid_app_with_event_log_modifiers.args_bucket_broker.events
                if _repid_app_with_event_log_modifiers.args_bucket_broker is not None
                else []
            )
            + (
                _repid_app_with_event_log_modifiers.result_bucket_broker.events
                if _repid_app_with_event_log_modifiers.result_bucket_broker is not None
                else []
            )
        ),
        key=lambda x: x.perf_counter,
    )


@pytest.fixture()
def repid_get_mocked_actor(
    _construct_repid_router_from_markers: Router,
) -> GetMockedActorT:
    return lambda x: _construct_repid_router_from_markers.actors[x].fn  # type: ignore[return-value]


@dataclass
class EventLogModifiers:
    message_broker: EventLogModifier
    args_bucket_broker: EventLogModifier | None
    result_bucket_broker: EventLogModifier | None


@pytest.fixture()
def repid_get_in_memory_queue(repid_connection: Connection) -> GetInMemoryQueueT:
    def _get_queue(queue: Queue | str) -> DummyQueue | None:
        if not isinstance(repid_connection.message_broker, InMemoryMessageBroker):
            return None  # pragma: no cover
        return repid_connection.message_broker.queues.get(
            queue.name if isinstance(queue, Queue) else queue,
        )

    return _get_queue


@pytest.fixture()
async def repid_declare_all_known_queues(  # noqa: PT004
    _construct_repid_router_from_markers: Router,
) -> None:
    await Worker(routers=[_construct_repid_router_from_markers]).declare_all_queues()


@pytest.fixture()
def _repid_app_with_event_log_modifiers(repid_app: Repid) -> EventLogModifiers:  # noqa: PT005
    return EventLogModifiers(
        message_broker=EventLogModifier(repid_app.connection.message_broker),
        args_bucket_broker=(
            EventLogModifier(repid_app.connection.args_bucket_broker)
            if repid_app.connection.args_bucket_broker is not None
            else None
        ),
        result_bucket_broker=(
            EventLogModifier(repid_app.connection.results_bucket_broker)
            if repid_app.connection.results_bucket_broker is not None
            else None
        ),
    )


@pytest.fixture()
def _repid_app_with_worker_modifier(  # noqa: PT005
    repid_app: Repid,
    _construct_repid_router_from_markers: Router,
) -> RunWorkerOnEnqueueModifier:
    return RunWorkerOnEnqueueModifier(
        repid_app.connection.message_broker,
        lambda: Worker(
            routers=[_construct_repid_router_from_markers],
            messages_limit=1,
            handle_signals=[],
            auto_declare=False,
            _connection=repid_app.connection,
        ),
    )


@pytest.fixture()
def _repid_app_with_modifiers(  # noqa: PT005
    repid_app: Repid,
    _repid_app_with_event_log_modifiers: EventLogModifiers,
    _repid_app_with_worker_modifier: RunWorkerOnEnqueueModifier,
) -> Repid:
    return repid_app


@pytest.fixture()
def _construct_repid_router_from_markers(request: pytest.FixtureRequest) -> Router:  # noqa: PT005
    router = Router()

    for mark in request.node.iter_markers("repid"):
        if (routers := mark.kwargs.get("routers")) is not None and isinstance(
            routers,
            collections.abc.Sequence,
        ):
            for r in routers:
                if not isinstance(r, Router):
                    pytest.fail(
                        f"Expected argument of the repid marker to contain a Router, received {type(r)}.",
                    )
                router.include_router(r)

    new_actors: dict[str, ActorData] = {}
    for name, actor_data in router.actors.items():
        new_actors[name] = actor_data._replace(fn=MagicMock(wraps=actor_data.fn))
    router.actors = new_actors

    return router
