from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from repid.connection import Connection
from repid.data import ArgsBucket, Message, PrioritiesT, ResultBucket, ResultMetadata
from repid.main import Repid
from repid.queue import Queue
from repid.utils import VALID_ID, VALID_NAME, unix_time


class Job:
    """Describes how and when the job should be executed."""

    __slots__ = (
        "id_",
        "name",
        "queue",
        "priority",
        "deferred_until",
        "deferred_by",
        "cron",
        "retries",
        "timeout",
        "created",
        "ttl",
        "args",
        "use_args_bucketer",
        "result_metadata",
        "_conn",
    )

    def __init__(
        self,
        name: str,
        queue: str | Queue = "default",
        priority: PrioritiesT = PrioritiesT.MEDIUM,
        deferred_until: datetime | int | None = None,
        deferred_by: timedelta | int | None = None,
        cron: str | None = None,
        retries: int = 1,
        timeout: int = 600,
        ttl: int | None = None,
        id_: str | None = None,
        args: ArgsBucket | None = None,
        use_args_bucketer: bool | None = None,
        result_metadata: ResultMetadata | None = None,
        _connection: Connection | None = None,
    ):
        self._conn = _connection or Repid.get_default_connection()

        if not self._conn.messager.supports_delayed_messages and self.is_deferred:
            raise ValueError("Deferred jobs are not supported by this connection.")

        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Job name must start with a letter or an underscore "
                "followed by letters, digits, dashes or underscores."
            )
        self.name = name

        if id_ is not None and not VALID_ID.fullmatch(id_):
            raise ValueError("Job id must contain only letters, numbers, dashes and underscores.")
        self.id_ = id_  # if id is not None -> job is unique

        if not isinstance(queue, Queue):
            self.queue = Queue(queue, self._conn)
        else:
            self.queue = queue

        self.priority = priority

        if (cron is not None) and (deferred_by is not None):
            raise ValueError("Usage of 'cron' AND 'deferred_by' together is prohibited.")

        if isinstance(deferred_until, datetime):
            deferred_until = int(deferred_until.timestamp())
        self.deferred_until = deferred_until

        if isinstance(deferred_by, timedelta):
            deferred_by = int(deferred_by.seconds)
        self.deferred_by = deferred_by

        self.cron = cron

        if retries < 1:
            raise ValueError("Retries must be greater than or equal to 1.")
        self.retries = retries

        if timeout < 1:
            raise ValueError("Execution timeout must be greater than or equal to 1 second.")
        self.timeout = timeout

        if ttl is not None and ttl < 1:
            raise ValueError("TTL must be greater than or equal to 1 second.")
        self.ttl = ttl

        self.result_metadata = result_metadata

        self.args = args

        if use_args_bucketer is None:
            self.use_args_bucketer = self._conn.args_bucketer is not None
        else:
            self.use_args_bucketer = use_args_bucketer

        self.created = unix_time()

    async def enqueue(self, store_args: bool = True, declare_queue: bool = True) -> Message:
        if store_args and self.use_args_bucketer and self.args is not None:
            await self._conn.ab.store_bucket(self.args)
        if declare_queue:
            await self.queue.declare()
        msg = self._to_message()
        await self._conn.messager.enqueue(msg)
        return msg

    @property
    def is_deferred(self) -> bool:
        return any((self.deferred_until, self.deferred_by, self.cron))

    @property
    def is_unique(self) -> bool:
        return self.id_ is not None

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return unix_time() > self.created + self.ttl
        return False

    @property
    async def result(self) -> ResultBucket | None:
        if self.result_metadata is None:
            raise ValueError("Result metadata is not set.")
        data = await self._conn.rb.get_bucket(self.result_metadata.id_)
        if isinstance(data, ResultBucket):
            return data
        return None

    def _to_message(self) -> Message:
        args_id = None
        simple_args = None
        simple_kwargs = None
        if self.args is not None:
            if self.use_args_bucketer:
                args_id = self.args.id_
            else:
                simple_args = self.args.args
                simple_kwargs = self.args.kwargs

        result_bucket_id = self.result_metadata.id_ if self.result_metadata is not None else None
        result_bucket_ttl = self.result_metadata.ttl if self.result_metadata is not None else None

        msg = Message(
            id_=self.id_ or uuid.uuid4().hex,
            topic=self.name,
            queue=self.queue.name,
            priority=self.priority.value,
            execution_timeout=self.timeout,
            retries=self.retries,
            tried=0,
            args_bucket_id=args_id,
            simple_args=simple_args,
            simple_kwargs=simple_kwargs,
            result_bucket_id=result_bucket_id,
            result_bucket_ttl=result_bucket_ttl,
            delay_until=self.deferred_until,
            defer_by=self.deferred_by,
            cron=self.cron,
            ttl=self.ttl,
            timestamp=unix_time(),
        )
        return msg

    def __str__(self) -> str:
        return f"Job(name={self.name}, queue={self.queue.name})"
