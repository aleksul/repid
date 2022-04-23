from repid import _default_connection
from repid.connections.connection import Connection


class Repid:
    """Main class. Mostly useful for producer.
    Helps enqueueing new jobs, get existing jobs and queues, as well as pop jobs.
    """

    def __init__(self, dsn: str):
        if dsn.startswith("redis") or dsn.startswith("rediss"):
            from repid.connections.redis_conn import RedisMessaging, RedisBucketing

            _default_connection = RedisConnection(dsn)
        # redis rediss
        # kafka
        # amqp amqps

        pass
