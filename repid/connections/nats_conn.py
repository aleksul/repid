import nats


class NatsMessaging:
    supports_delayed_messages = True
    queue_type = "FIFO"
    priorities_distribution = "10/3/1"

    def __init__(self, connection: str):
        pass

    raise NotImplementedError
