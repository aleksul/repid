from typing import Optional, Union

from repid import _default_connection  # noqa: F401
from repid.connections.connection import Connection


class Repid:
    """Main class. Mostly useful for producer.
    Helps enqueueing new jobs, get existing jobs and queues, as well as pop jobs.
    """

    def __init__(
        self,
        dsn: str,
        args_bucketing: Union[str, bool, None] = False,
        result_bucketing: Union[str, bool, None] = False,
    ):
        global _default_connection

        args_dsn: Optional[str] = None
        if args_bucketing is True:
            args_dsn = dsn

        result_dsn: Optional[str] = None
        if result_bucketing is True:
            result_dsn = dsn

        dsns = dict(
            messager=dsn,
            args_bucketer=args_dsn,
            result_bucketer=result_dsn,
        )

        connections = dict()

        for name, conn in dsns.items():
            if conn is None:
                continue
            connections[name] = self.__get_connection_from_string(
                conn, bucketing=True if name != "messager" else False
            )
        _default_connection = Connection(**connections)
        self.__conn = _default_connection

    def __get_connection_from_string(self, connection: str, bucketing: bool = False):
        if connection.startswith("redis") or connection.startswith("rediss"):
            if not bucketing:
                from repid.connections.redis_conn import RedisMessaging

                return RedisMessaging(connection)
            else:
                from repid.connections.redis_conn import RedisBucketing

                return RedisBucketing(connection)
        elif connection.startswith("kafka"):
            if bucketing:
                raise ValueError("Kafka only serves as a message broker.")

            from repid.connections.kafka_conn import KafkaMessaging

            return KafkaMessaging(connection)
        elif connection.startswith("amqp") or connection.startswith("amqps"):
            if bucketing:
                raise ValueError("AMQP only serves as a message broker.")

            from repid.connections.amqp_conn import AMQPMessaging

            return AMQPMessaging(connection)
        elif connection.startswith("sqs"):
            if bucketing:
                raise ValueError("SQS only serves as a message broker.")

            from repid.connections.sqs_conn import SQSMessaging

            return SQSMessaging(connection)
        raise ValueError(f"Unknown connection type: {connection = }")
