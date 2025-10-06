from __future__ import annotations

import sys
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

if TYPE_CHECKING:
    from .channels import Channel
    from .common import ReferenceModel
    from .components import Components
    from .info import Info
    from .operations import Operation
    from .servers import Server


class AsyncAPI3Schema(TypedDict, total=False):
    info: Required[Info]
    asyncapi: str
    id: str
    servers: Mapping[str, ReferenceModel | Server] | None
    default_content_type: str
    channels: Mapping[str, ReferenceModel | Channel] | None
    operations: Mapping[str, ReferenceModel | Operation] | None
    components: Components
