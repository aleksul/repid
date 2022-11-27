from dataclasses import dataclass, field
from typing import Union
from uuid import uuid4


@dataclass(frozen=True)
class ResultMetadata:
    id_: str = field(default_factory=lambda: uuid4().hex)
    ttl: Union[int, None] = 86400
