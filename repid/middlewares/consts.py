from itertools import product

AVAILABLE_FUNCTIONS = (
    "consume",
    "enqueue",
    "queue_declare",
    "queue_flush",
    "queue_delete",
    "ack",
    "nack",
    "reject",
    "requeue",
    "get_bucket",
    "store_bucket",
    "delete_bucket",
)

POSSIBLE_EVENT_NAMES = set(
    map(lambda i: i[0] + i[1], product(("before_", "after_"), AVAILABLE_FUNCTIONS))
)