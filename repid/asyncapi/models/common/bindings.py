from __future__ import annotations

from typing import Any, Literal, TypedDict


class Mqtt(TypedDict, total=False):
    binding_version: Literal["0.2.0"]


class Kafka(TypedDict, total=False):
    binding_version: Literal["0.5.0", "0.4.0", "0.3.0"]


class Jms(TypedDict, total=False):
    binding_version: Literal["0.0.1"]


class Ibmmq(TypedDict, total=False):
    binding_version: Literal["0.1.0"]


class Solace(TypedDict, total=False):
    binding_version: Literal["0.4.0", "0.3.0", "0.2.0"]


class Pulsar(TypedDict, total=False):
    binding_version: Literal["0.1.0"]


class Http(TypedDict, total=False):
    binding_version: Literal["0.2.0", "0.3.0"]


class Amqp(TypedDict, total=False):
    binding_version: Literal["0.3.0"]


class Anypointmq(TypedDict, total=False):
    binding_version: Literal["0.0.1"]


class Googlepubsub(TypedDict, total=False):
    binding_version: Literal["0.2.0"]


class Ws(TypedDict, total=False):
    binding_version: Literal["0.1.0"]


class Sns(TypedDict, total=False):
    binding_version: Literal["0.1.0"]


class Sqs(TypedDict, total=False):
    binding_version: Literal["0.2.0"]


class Nats(TypedDict, total=False):
    binding_version: Literal["0.1.0"]


class ServerBindingsObject(TypedDict, total=False):
    http: Any
    ws: Any
    amqp: Any
    amqp1: Any
    mqtt: Mqtt
    kafka: Kafka
    anypointmq: Any
    nats: Any
    jms: Jms
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Ibmmq
    solace: Solace
    googlepubsub: Any
    pulsar: Pulsar


class OperationBindingsObject(TypedDict, total=False):
    http: Http
    ws: Any
    amqp: Amqp
    amqp1: Any
    mqtt: Mqtt
    kafka: Kafka
    anypointmq: Any
    nats: Nats
    jms: Any
    sns: Sns
    sqs: Sqs
    stomp: Any
    redis: Any
    ibmmq: Any
    solace: Solace
    googlepubsub: Any


class ChannelBindingsObject(TypedDict, total=False):
    http: Any
    ws: Ws
    amqp: Amqp
    amqp1: Any
    mqtt: Any
    kafka: Kafka
    anypointmq: Anypointmq
    nats: Any
    jms: Jms
    sns: Sns
    sqs: Sqs
    stomp: Any
    redis: Any
    ibmmq: Ibmmq
    solace: Any
    googlepubsub: Googlepubsub
    pulsar: Pulsar


class MessageBindingsObject(TypedDict, total=False):
    http: Http
    ws: Any
    amqp: Amqp
    amqp1: Any
    mqtt: Mqtt
    kafka: Kafka
    anypointmq: Anypointmq
    nats: Any
    jms: Jms
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Ibmmq
    solace: Any
    googlepubsub: Googlepubsub
