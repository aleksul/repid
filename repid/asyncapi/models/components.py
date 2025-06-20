from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from .channels import Channel, ChannelBindingsObject, ChannelParameter
    from .common import (
        CorrelationId,
        ExternalDocs,
        MessageBindingsObject,
        MessageObject,
        MessageTrait,
        ReferenceModel,
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
    serverVariables: Mapping[str, ReferenceModel | ServerVariable]
    operations: Mapping[str, ReferenceModel | Operation]
    messages: Mapping[str, ReferenceModel | MessageObject]
    securitySchemes: Mapping[str, ReferenceModel | Any]
    parameters: Mapping[str, ReferenceModel | ChannelParameter]
    correlationIds: Mapping[str, ReferenceModel | CorrelationId]
    operationTraits: Mapping[str, ReferenceModel | OperationTrait]
    messageTraits: Mapping[str, ReferenceModel | MessageTrait]
    replies: Mapping[str, ReferenceModel | OperationReply]
    replyAddresses: Mapping[str, ReferenceModel | OperationReplyAddress]
    serverBindings: Mapping[str, ReferenceModel | ServerBindingsObject]
    channelBindings: Mapping[str, ReferenceModel | ChannelBindingsObject]
    operationBindings: Mapping[str, ReferenceModel | OperationBindingsObject]
    messageBindings: Mapping[str, ReferenceModel | MessageBindingsObject]
    tags: Mapping[str, ReferenceModel | Tag]
    externalDocs: Mapping[str, ReferenceModel | ExternalDocs]
