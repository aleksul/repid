from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict

from typing_extensions import Required

from .channels import Channel
from .common import ReferenceModel
from .components import Components
from .info import Info
from .operations import Operation
from .servers import Server

Servers = Mapping[str, ReferenceModel | Server] | None
Channels = Mapping[str, ReferenceModel | Channel] | None
Operations = Mapping[str, ReferenceModel | Operation] | None


class Asyncapi300Schema(TypedDict, total=False):
    info: Required[Info]
    asyncapi: str
    id: str
    servers: Servers
    default_content_type: str
    channels: Channels
    operations: Operations
    components: Components
