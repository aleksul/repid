import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Tuple, Union

import orjson

from repid.data import ParametersT, PrioritiesT, ResultBucketT, RoutingKeyT
from repid.main import DEFAULT_CONNECTION
from repid.queue import Queue
from repid.serializer import default_serializer
from repid.utils import SLOTS_DATACLASS, VALID_ID, VALID_NAME

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.serializer import SerializerT


@dataclass(**SLOTS_DATACLASS)
class Job:
    ARGS_SERIALIZER: ClassVar["SerializerT"] = default_serializer

    name: str
    queue: Union[str, Queue] = "default"
    priority: PrioritiesT = PrioritiesT.MEDIUM
    id_: Union[str, None] = None

    deferred_until: Union[datetime, None] = None
    deferred_by: Union[timedelta, None] = None
    cron: Union[str, None] = None

    retries: int = 1
    timeout: timedelta = field(default_factory=lambda: timedelta(minutes=10))

    ttl: Union[timedelta, None] = None
    timestamp: datetime = field(default_factory=datetime.now, init=False)

    args_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    args_ttl: Union[timedelta, None] = None
    args: Any = None
    use_args_bucketer: Union[bool, None] = None

    result_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    result_ttl: Union[timedelta, None] = field(default_factory=lambda: timedelta(days=1))
    store_result: Union[bool, None] = None

    _connection: "Connection" = field(default_factory=lambda: DEFAULT_CONNECTION.get(), repr=False)

    def __post_init__(self) -> None:
        if not VALID_NAME.fullmatch(self.name):
            raise ValueError(
                "Job name must start with a letter or an underscore "
                "followed by letters, digits, dashes or underscores."
            )

        if self.id_ is not None and not VALID_ID.fullmatch(self.id_):
            raise ValueError("Job id must contain only letters, numbers, dashes and underscores.")

        self.queue: Queue
        if not isinstance(self.queue, Queue):
            self.queue = Queue(self.queue, self._connection)

        if (self.cron is not None) and (self.deferred_by is not None):
            raise ValueError("Usage of 'cron' AND 'deferred_by' together is prohibited.")

        if self.deferred_by is not None and self.deferred_by.total_seconds() < 1:
            raise ValueError("Deferred_by must be greater than or equal to 1 second.")

        if self.retries < 1:
            raise ValueError("Retries must be greater than or equal to 1.")

        if self.timeout.total_seconds() < 1:
            raise ValueError("Execution timeout must be greater than or equal to 1 second.")

        if self.ttl is not None and self.ttl.total_seconds() < 1:
            raise ValueError("TTL must be greater than or equal to 1 second.")

        self.args: str = Job.ARGS_SERIALIZER(self.args)

        if self.use_args_bucketer is None:
            self.use_args_bucketer = self._connection.args_bucket_broker is not None

        if self.result_id is not None and not VALID_ID.fullmatch(self.result_id):
            raise ValueError(
                "Result_id must contain only letters, numbers, dashes and underscores."
            )

        if self.result_ttl is not None and self.result_ttl.total_seconds() < 1:
            raise ValueError("Result_ttl must be greater than or equal to 1 second.")

        if self.store_result is None:
            self.store_result = self._connection.results_bucket_broker is not None

    async def enqueue(self, store_args: bool = True) -> Tuple[RoutingKeyT, str, ParametersT]:
        stored_args = False
        if store_args and self.use_args_bucketer and self.args is not None:
            bucket = self._connection._ab.BUCKET_CLASS(data=self.args, ttl=self.args_ttl)
            await self._connection._ab.store_bucket(self.args_id, bucket)
            stored_args = True
        key = self._connection.message_broker.ROUTING_KEY_CLASS(
            id_=self.id_ or uuid.uuid4().hex,
            topic=self.name,
            queue=self.queue.name,  # type: ignore[union-attr]
            priority=self.priority.value,
        )
        parameters = self._connection.message_broker.PARAMETERS_CLASS(
            execution_timeout=self.timeout,
            retries=self._connection.message_broker.PARAMETERS_CLASS.RETRIES_CLASS(
                max_amount=self.retries
            ),
            result=self._connection.message_broker.PARAMETERS_CLASS.RESULT_CLASS(
                id_=self.result_id, ttl=self.result_ttl
            )
            if self.store_result
            else None,
            delay=self._connection.message_broker.PARAMETERS_CLASS.DELAY_CLASS(
                delay_until=self.deferred_until, defer_by=self.deferred_by, cron=self.cron
            ),
            timestamp=self.timestamp,
            ttl=self.ttl,
        )
        await self._connection.message_broker.enqueue(
            key,
            orjson.dumps(dict(__repid_payload_id=self.args_id)).decode()
            if stored_args
            else self.args,
            parameters,
        )
        return (key, self.args, parameters)

    @property
    def is_deferred(self) -> bool:
        return any((self.deferred_until, self.deferred_by, self.cron))

    @property
    def is_unique(self) -> bool:
        return self.id_ is not None

    @property
    def is_overdue(self) -> bool:
        if self.ttl is not None:
            return datetime.now() > self.timestamp + self.ttl
        return False

    @property
    async def result(self) -> Union[ResultBucketT, None]:
        if self.result_id is None:
            raise ValueError("Result id is not set.")
        data = await self._connection._rb.get_bucket(self.result_id)
        if isinstance(data, ResultBucketT):
            return data
        return None
