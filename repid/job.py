import uuid
from datetime import datetime, timedelta
from typing import Union

from repid.connection import Connection
from repid.data import ArgsBucket, Message, PrioritiesT, ResultBucket
from repid.data.messages import (
    ArgsBucketMetadata,
    ResultBucketMetadata,
    SimpleArgsBucket,
)
from repid.main import DEFAULT_CONNECTION
from repid.queue import Queue
from repid.utils import VALID_ID, VALID_NAME, unix_time

ArgsBucketT = Union[SimpleArgsBucket, ArgsBucketMetadata, None]


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
        "__args_bucket_metadata",
        "__simple_args_bucket",
        "__result_bucket_metadata",
        "__conn",
    )

    def __init__(
        self,
        name: str,
        queue: Union[str, Queue] = "default",
        priority: PrioritiesT = PrioritiesT.MEDIUM,
        deferred_until: Union[datetime, int, None] = None,
        deferred_by: Union[timedelta, int, None] = None,
        cron: Union[str, None] = None,
        retries: int = 1,
        timeout: int = 600,
        ttl: Union[int, None] = None,
        id_: Union[str, None] = None,
        _connection: Union[Connection, None] = None,
    ):
        self.__conn = _connection or DEFAULT_CONNECTION.get()

        if not self.__conn.messager.supports_delayed_messages and self.is_deferred:
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
            self.queue = Queue(queue, self.__conn)
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

        self.__args_bucket_metadata: Union[ArgsBucketMetadata, None] = None
        self.__simple_args_bucket: Union[SimpleArgsBucket, None] = None
        self.__result_bucket_metadata: Union[ResultBucketMetadata, None] = None
        if self.__conn.results_bucketer is not None:
            self.__result_bucket_metadata = ResultBucketMetadata(id_=uuid.uuid4().hex, ttl=86400)
        self.created = unix_time()

    async def enqueue(self) -> Message:
        msg = self._to_message()
        await self.__conn.messager.enqueue(msg)
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
    def args_metadata(self) -> Union[ArgsBucketMetadata, None]:
        return self.__args_bucket_metadata

    @args_metadata.setter
    def args_metadata(self, value: Union[ArgsBucketMetadata, None]) -> None:
        self.__args_bucket_metadata = None
        if isinstance(value, ArgsBucketMetadata):
            self.__args_bucket_metadata = value
        self.__simple_args_bucket = None

    @property
    def simple_args(self) -> Union[SimpleArgsBucket, None]:
        return self.__simple_args_bucket

    @simple_args.setter
    def simple_args(self, value: Union[SimpleArgsBucket, None]) -> None:
        self.__simple_args_bucket = None
        if isinstance(value, SimpleArgsBucket):
            self.__simple_args_bucket = value
        self.__args_bucket_metadata = None

    @property
    def result_metadata(self) -> Union[ResultBucketMetadata, None]:
        return self.__result_bucket_metadata

    @result_metadata.setter
    def result_metadata(self, value: Union[ResultBucketMetadata, None]) -> None:
        self.__result_bucket_metadata = None
        if isinstance(value, ResultBucketMetadata):
            self.__result_bucket_metadata = value

    @property
    async def args(self) -> Union[SimpleArgsBucket, ArgsBucket, None]:
        if self.simple_args is not None:
            return self.simple_args
        if self.args_metadata is None:
            raise ValueError("Args metadata object is not configured.")
        if self.__conn.args_bucketer is None:
            raise ConnectionError("Args bucketer is not configured.")
        data = await self.__conn.args_bucketer.get_bucket(
            id_=self.args_metadata.id_,
        )
        if isinstance(data, ArgsBucket):
            return data
        return None

    @args.setter
    async def args(self, value: ArgsBucket) -> None:
        metadata = ArgsBucketMetadata(id_=value.id_)
        self.args_metadata = metadata
        if self.__conn.args_bucketer is None:
            raise ConnectionError("Args bucketer is not configured.")
        await self.__conn.args_bucketer.store_bucket(value)

    @args.deleter
    async def args(self) -> None:
        if self.args_metadata is None:
            raise ValueError("Args metadata object is not configured.")
        if self.__conn.args_bucketer is None:
            raise ConnectionError("Args bucketer is not configured.")
        await self.__conn.args_bucketer.delete_bucket(self.args_metadata.id_)

    @property
    async def result(self) -> Union[ResultBucket, None]:
        if self.result_metadata is None:
            raise ValueError("Result metadata object is not configured.")
        if self.__conn.results_bucketer is None:
            raise ConnectionError("Results bucketer is not configured.")
        data = await self.__conn.results_bucketer.get_bucket(
            self.result_metadata.id_,
        )
        if isinstance(data, ResultBucket):
            return data
        return None

    def _to_message(self) -> Message:
        msg = Message(
            id_=self.id_ or uuid.uuid4().hex,
            topic=self.name,
            queue=self.queue.name,
            priority=self.priority,
            execution_timeout=self.timeout,
            retries=self.retries,
            tried=0,
            args_bucket=self.args_metadata or self.simple_args,
            result_bucket=self.result_metadata,
            delay_until=self.deferred_until,
            defer_by=self.deferred_by,
            cron=self.cron,
            ttl=self.ttl,
            timestamp=unix_time(),
        )
        if self.is_deferred and msg.delay_until is None:
            msg.delay_until = msg.next_execution_time
        return msg

    def __str__(self) -> str:
        return f"Job(name={self.name}, queue={self.queue.name})"
