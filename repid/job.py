from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from repid._utils import VALID_ID, VALID_NAME, _ArgsBucketInMessageId
from repid.config import Config
from repid.data import ParametersT, PrioritiesT, ResultBucketT, RoutingKeyT
from repid.main import Repid
from repid.queue import Queue

if TYPE_CHECKING:
    from repid.connection import Connection


class Job:
    __slots__ = (
        "name",
        "queue",
        "priority",
        "id_",
        "deferred_until",
        "deferred_by",
        "cron",
        "retries",
        "timeout",
        "ttl",
        "timestamp",
        "args_id",
        "args_id_set",
        "args_ttl",
        "args",
        "use_args_bucketer",
        "result_id",
        "result_ttl",
        "store_result",
        "_conn",
    )

    name: str
    queue: Queue
    priority: PrioritiesT
    id_: str | None
    deferred_until: datetime | None
    deferred_by: timedelta | None
    cron: str | None
    retries: int
    timeout: timedelta
    ttl: timedelta | None
    timestamp: datetime
    args_id: str
    args_id_set: bool
    args_ttl: timedelta | None
    args: str | None
    use_args_bucketer: bool
    result_id: str
    result_ttl: timedelta | None
    store_result: bool
    _conn: Connection

    def __init__(  # noqa: C901
        self,
        name: str,
        queue: str | Queue = "default",
        priority: PrioritiesT = PrioritiesT.MEDIUM,
        id_: str | None = None,
        deferred_until: datetime | None = None,
        deferred_by: timedelta | None = None,
        cron: str | None = None,
        retries: int = 0,
        timeout: timedelta = timedelta(minutes=10),
        ttl: timedelta | None = None,
        args_id: str | None = None,  # default: uuid4
        args_ttl: timedelta | None = None,
        args: Any = None,
        use_args_bucketer: bool | None = None,
        result_id: str | None = None,  # default: uuid4
        result_ttl: timedelta | None = timedelta(days=1),
        store_result: bool | None = None,
        _connection: Connection | None = None,
    ) -> None:
        self._conn = _connection or Repid.get_magic_connection()

        self.name = name
        if not VALID_NAME.fullmatch(self.name):
            raise ValueError(
                "Job name must start with a letter or an underscore "
                "followed by letters, digits, dashes or underscores.",
            )

        self.queue = queue if isinstance(queue, Queue) else Queue(queue, self._conn)

        self.priority = priority

        self.id_ = id_
        if self.id_ is not None and not VALID_ID.fullmatch(self.id_):
            raise ValueError("Job id must contain only letters, numbers, dashes and underscores.")

        self.deferred_until = deferred_until
        self.deferred_by = deferred_by
        self.cron = cron
        if (self.cron is not None) and (self.deferred_by is not None):
            raise ValueError("Usage of 'cron' AND 'deferred_by' together is prohibited.")
        if self.deferred_by is not None and self.deferred_by.total_seconds() < 1:
            raise ValueError("Deferred_by must be greater than or equal to 1 second.")

        self.retries = retries
        if self.retries < 0:
            raise ValueError("Retries must be greater than or equal to 0.")

        self.timeout = timeout
        if self.timeout.total_seconds() < 1:
            raise ValueError("Execution timeout must be greater than or equal to 1 second.")

        self.ttl = ttl
        if self.ttl is not None and self.ttl.total_seconds() < 1:
            raise ValueError("TTL must be greater than or equal to 1 second.")

        self.args_id = args_id if isinstance(args_id, str) else uuid.uuid4().hex
        self.args_id_set = bool(args_id)
        self.args_ttl = args_ttl
        if self.args_ttl is not None and self.args_ttl.total_seconds() < 1:
            raise ValueError("Args TTL must be greater than or equal to 1 second.")
        self.args = None if args is None else Config.SERIALIZER(args)
        self.use_args_bucketer = (
            (self._conn.args_bucket_broker is not None)
            if use_args_bucketer is None
            else use_args_bucketer
        )

        self.result_id = result_id if isinstance(result_id, str) else uuid.uuid4().hex
        self.result_ttl = result_ttl
        if self.result_id is not None and not VALID_ID.fullmatch(self.result_id):
            raise ValueError(
                "Result id must contain only letters, numbers, dashes and underscores.",
            )
        if self.result_ttl is not None and self.result_ttl.total_seconds() < 1:
            raise ValueError("Result TTL must be greater than or equal to 1 second.")
        self.store_result = (
            (self._conn.results_bucket_broker is not None) if store_result is None else store_result
        )

        self.timestamp = datetime.now()

    def _construct_routing_key(self) -> RoutingKeyT:
        return self._conn.message_broker.ROUTING_KEY_CLASS(
            id_=self.id_ or uuid.uuid4().hex,
            topic=self.name,
            queue=self.queue.name,
            priority=self.priority.value,
        )

    def _construct_parameters(self) -> ParametersT:
        return self._conn.message_broker.PARAMETERS_CLASS(
            execution_timeout=self.timeout,
            retries=self._conn.message_broker.PARAMETERS_CLASS.RETRIES_CLASS(
                max_amount=self.retries,
            ),
            result=(
                self._conn.message_broker.PARAMETERS_CLASS.RESULT_CLASS(
                    id_=self.result_id,
                    ttl=self.result_ttl,
                )
                if self.store_result
                else None
            ),
            delay=self._conn.message_broker.PARAMETERS_CLASS.DELAY_CLASS(
                delay_until=self.deferred_until,
                defer_by=self.deferred_by,
                cron=self.cron,
            ),
            timestamp=self.timestamp,
            ttl=self.ttl,
        )

    async def _construct_args(self) -> str:
        if self.use_args_bucketer and self.args is not None:
            bucket = self._conn._ab.BUCKET_CLASS(data=self.args, ttl=self.args_ttl)
            await self._conn._ab.store_bucket(self.args_id, bucket)
            return _ArgsBucketInMessageId.construct(self.args_id)
        if self.args_id_set:
            return _ArgsBucketInMessageId.construct(self.args_id)
        return self.args or ""

    async def enqueue(self) -> tuple[RoutingKeyT, str, ParametersT]:
        key = self._construct_routing_key()
        parameters = self._construct_parameters()
        args = await self._construct_args()
        await self._conn.message_broker.enqueue(key, args, parameters)
        return (key, self.args or "", parameters)

    @property
    def is_deferred(self) -> bool:
        return any((self.deferred_until, self.deferred_by, self.cron))

    @property
    def is_unique(self) -> bool:
        return self.id_ is not None

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return datetime.now(tz=self.timestamp.tzinfo) > self.timestamp + self.ttl
        return False

    @property
    async def result(self) -> ResultBucketT | None:
        data = await self._conn._rb.get_bucket(self.result_id)
        if isinstance(data, ResultBucketT):
            return data
        return None
