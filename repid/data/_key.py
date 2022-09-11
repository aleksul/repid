from uuid import uuid4

from pydantic import BaseModel, Field

from repid.data.protocols import PrioritiesT
from repid.utils import VALID_ID, VALID_NAME


class RoutingKey(BaseModel):
    id_: str = Field(default_factory=lambda: uuid4().hex, regex=str(VALID_ID))
    topic: str = Field(..., regex=str(VALID_NAME))
    queue: str = Field("default", regex=str(VALID_NAME))
    priority: int = PrioritiesT.MEDIUM.value
