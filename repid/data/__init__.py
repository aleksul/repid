from typing import Union

from buckets import ArgsBucket, Bucket, ResultBucket
from messages import DeferredByMessage, DeferredCronMessage, DeferredMessage, Message
from mixins import StructWithParams, Timestamp
from priorities import PrioritiesT

AnyMessageT = Union[Message, DeferredMessage, DeferredByMessage, DeferredCronMessage]
AnyBucketT = Union[ArgsBucket, ResultBucket]
