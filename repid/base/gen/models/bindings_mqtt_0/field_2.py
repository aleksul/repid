# generated by datamodel-codegen:
#   filename:  3.0.0-without-$id.json
#   timestamp: 2024-12-18T11:23:24+00:00

from __future__ import annotations

from typing import Literal, TypedDict

from typing_extensions import NotRequired

from .. import ReferenceModel, Schema


class LastWill(TypedDict):
    topic: NotRequired[str]
    qos: NotRequired[Literal[0, 1, 2]]
    message: NotRequired[str]
    retain: NotRequired[bool]


class Field0Server(TypedDict):
    client_id: NotRequired[str]
    clean_session: NotRequired[bool]
    last_will: NotRequired[LastWill]
    keep_alive: NotRequired[int]
    session_expiry_interval: NotRequired[int | Schema | ReferenceModel]
    maximum_packet_size: NotRequired[int | Schema | ReferenceModel]
    binding_version: NotRequired[Literal['0.2.0']]


class Field0Message(TypedDict):
    payload_format_indicator: NotRequired[Literal[0, 1]]
    correlation_data: NotRequired[Schema | ReferenceModel]
    content_type: NotRequired[str]
    response_topic: NotRequired[str | Schema | ReferenceModel]
    binding_version: NotRequired[Literal['0.2.0']]


class Field0Operation(TypedDict):
    qos: NotRequired[Literal[0, 1, 2]]
    retain: NotRequired[bool]
    message_expiry_interval: NotRequired[int | Schema | ReferenceModel]
    binding_version: NotRequired[Literal['0.2.0']]
