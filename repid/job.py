import uuid
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Literal, Optional, Type, Union

import orjson
from pydantic import BaseModel

# from . import _connection_manager
# from .connections.connection import Connection
# from .queue import Queue
# from .utils import VALID_NAME, current_unix_time, orjson_dumper


class JobStatus(Enum):
    QUEUED = auto()
    PROCESSING = auto()
    DONE = auto()
    DEAD = auto()
    NOT_FOUND = auto()


class Job:
    """Describes how and when the job should be executed."""

    __slots__ = (
        "id_",
        "name",
        "queue",
        "priority",
        "deferred_until",
        "deferred_by",
        "retries",
        "timeout",
        "created",
        "updated",
        "ttl",
        "_request_schema",
        "_request_data",
        "_response_schema",
        "_created",
        "_updated",
        "_dead",
        "_retries_left",
        "_next_exec_time",
        "__connection",
    )

    def __init__(
        self,
        name: str,
        queue: Union[str, Queue] = "default",
        priority: Literal["HIGH", "NORMAL", "LOW", "DEFERRED"] = "NORMAL",
        deferred_until: Union[datetime, int, None] = None,
        deferred_by: Union[timedelta, int, None] = None,
        retries: int = 1,
        timeout: int = 600,
        request_model: Optional[BaseModel] = None,
        response_model: Optional[Type[BaseModel]] = None,
        ttl: Optional[int] = None,
        id_: Optional[str] = None,
        _connection: Optional["Connection"] = None,
        **kwargs,
    ):
        if not VALID_NAME.fullmatch(name):
            raise ValueError(
                "Job name must start with a letter or an underscore"
                "followed by letters, digits, dashes or underscores."
            )
        self.name = name
        self.id_ = id_ or f"{name}:{uuid.uuid4().hex}"

        if not isinstance(queue, Queue):
            self.queue = Queue(queue)
        else:
            self.queue = queue

        self.priority = priority

        is_deferred: bool = any(v is not None for v in [deferred_by, deferred_until])
        if is_deferred and self.priority != "DEFERRED":
            raise ValueError("Deferred job must have priority='DEFERRED'.")

        if deferred_until is not None and deferred_by is not None:
            raise ValueError("Usage of 'deferred_until' AND 'deferred_by' together is prohibited.")

        self.deferred_until: Optional[int] = None
        if isinstance(deferred_until, datetime):
            self.deferred_until = int(deferred_until.timestamp())
        elif type(deferred_until) is int:
            self.deferred_until = deferred_until

        self.deferred_by: Optional[int] = None
        if isinstance(deferred_by, timedelta):
            self.deferred_by = int(deferred_by.seconds)
        elif type(deferred_by) is int:
            self.deferred_by = deferred_by

        if retries < 1:
            raise ValueError("Retries must be greater than or equal to 1.")
        self.retries = retries

        if timeout < 1:
            raise ValueError("Execution timeout must be greater than or equal to 1 second.")
        self.timeout = timeout

        if ttl is not None and ttl < 1:
            raise ValueError("TTL must be greater than or equal to 1 second.")
        self.ttl = ttl

        if request_model is not None:
            request_model.Config.json_dumps = orjson_dumper
            self._request_schema = request_model.schema_json()
            self._request_data = request_model.json()

        if response_model is not None:
            response_model.Config.json_dumps = orjson_dumper
            self._response_schema = response_model.schema_json()

        now = current_unix_time()
        self._created, self._updated = now, now

        self.__connection = _connection or _connection_manager.default_connection

        for k, v in kwargs.items():
            if k.startswith("_") and k in self.__slots__:
                setattr(self, k, v)

    async def enqueue(self):
        await self.__redis__.set(
            JOB_PREFIX + self._id,
            orjson.dumps(self.__as_dict__()),
            nx=True,
        )
        await self.queue.add_job(self._id, self.is_deferred)

    async def update(self):
        await self.queue.remove_job(self._id)
        await self.__redis__.set(
            JOB_PREFIX + self._id,
            orjson.dumps(self.__as_dict__()),
            xx=True,
        )
        await self.queue.add_job(self._id, self.is_deferred)

    async def delete(self) -> None:
        await self.__connection.delete_job(self.id_)

    @property
    def is_deferred(self) -> bool:
        return (self.defer_until is not None) or (self.defer_by is not None)

    @property
    async def is_deferred_already(self) -> bool:
        if self.defer_until is not None:
            if self.defer_until > int(datetime.utcnow().timestamp()):
                return False
        if self.defer_by is not None:
            if (res := await self.result) is not None:
                if res.finished_when + self.defer_by > int(datetime.utcnow().timestamp()):
                    return False
        return True

    @property
    async def status(self) -> JobStatus:
        if not await self.__redis__.exists(JOB_PREFIX + self._id):
            return JobStatus.NOT_FOUND
        if await self.queue.is_job_queued(self._id):
            return JobStatus.QUEUED
        if await self.result is not None:
            return JobStatus.DONE
        else:
            return JobStatus.IN_PROGRESS  # pragma: no cover

    @property
    async def result(self) -> Optional[JobResult]:
        raw = await self.__redis__.get(RESULT_PREFIX + self._id)
        return None if raw is None else JobResult(**orjson.loads(raw))
