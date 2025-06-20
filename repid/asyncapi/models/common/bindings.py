from __future__ import annotations

from typing import Any, TypedDict


class ServerBindingsObject(TypedDict, total=False):
    http: Any
    ws: Any
    amqp: Any
    amqp1: Any
    mqtt: Any
    kafka: Any
    anypointmq: Any
    nats: Any
    jms: Any
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Any
    solace: Any
    googlepubsub: Any
    pulsar: Any


class OperationBindingsObject(TypedDict, total=False):
    http: Any
    ws: Any
    amqp: Any
    amqp1: Any
    mqtt: Any
    kafka: Any
    anypointmq: Any
    nats: Any
    jms: Any
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Any
    solace: Any
    googlepubsub: Any


class ChannelBindingsObject(TypedDict, total=False):
    http: Any
    ws: Any
    amqp: Any
    amqp1: Any
    mqtt: Any
    kafka: Any
    anypointmq: Any
    nats: Any
    jms: Any
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Any
    solace: Any
    googlepubsub: Any
    pulsar: Any


class MessageBindingsObject(TypedDict, total=False):
    http: Any
    ws: Any
    amqp: Any
    amqp1: Any
    mqtt: Any
    kafka: Any
    anypointmq: Any
    nats: Any
    jms: Any
    sns: Any
    sqs: Any
    stomp: Any
    redis: Any
    ibmmq: Any
    solace: Any
    googlepubsub: Any
