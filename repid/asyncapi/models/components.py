from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict

from .channels import Channel, ChannelBindingsObject, Parameter
from .common import (
    CorrelationId,
    ExternalDocs,
    MessageBindingsObject,
    MessageObject,
    MessageTrait,
    ReferenceModel,
    SecurityScheme,
    Tag,
)
from .operations import (
    Operation,
    OperationBindingsObject,
    OperationReply,
    OperationReplyAddress,
    OperationTrait,
)
from .servers import Server, ServerBindingsObject, ServerVariable

AnySchema = Any


class Components(TypedDict, total=False):
    schemas: Mapping[str, AnySchema]
    servers: Mapping[str, ReferenceModel | Server]
    channels: Mapping[str, ReferenceModel | Channel]
    server_variables: Mapping[str, ReferenceModel | ServerVariable]
    operations: Mapping[str, ReferenceModel | Operation]
    messages: Mapping[str, ReferenceModel | MessageObject]
    security_schemes: Mapping[str, ReferenceModel | SecurityScheme]
    parameters: Mapping[str, ReferenceModel | Parameter]
    correlation_ids: Mapping[str, ReferenceModel | CorrelationId]
    operation_traits: Mapping[str, ReferenceModel | OperationTrait]
    message_traits: Mapping[str, ReferenceModel | MessageTrait]
    replies: Mapping[str, ReferenceModel | OperationReply]
    reply_addresses: Mapping[str, ReferenceModel | OperationReplyAddress]
    server_bindings: Mapping[str, ReferenceModel | ServerBindingsObject]
    channel_bindings: Mapping[str, ReferenceModel | ChannelBindingsObject]
    operation_bindings: Mapping[str, ReferenceModel | OperationBindingsObject]
    message_bindings: Mapping[str, ReferenceModel | MessageBindingsObject]
    tags: Mapping[str, ReferenceModel | Tag]
    external_docs: Mapping[str, ReferenceModel | ExternalDocs]
