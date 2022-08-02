from typing import Optional, Union

from repid.data import PrioritiesT, StructWithParams, Timestamp


class Message(StructWithParams, Timestamp):
    id_: str
    topic: str  # the same as actor's & job's name
    queue: str = "default"
    priority: PrioritiesT = PrioritiesT.MEDIUM
    retries_left: int = 1
    bucket: Union[str, "ArgsBucket", None] = None  # bucket_id or bucket itself
    result_id: Optional[str] = None
    result_ttl: Optional[int] = None
    timeout: int = 600


class DeferredMessage(Message):
    delay_until: int


class DeferredByMessage(DeferredMessage):
    defer_by: int


class DeferredCronMessage(DeferredMessage):
    cron: str
