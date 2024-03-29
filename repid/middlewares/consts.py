from itertools import product

WRAPPED = (
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
    "actor_run",
)

SUBSCRIBERS_NAMES = {i[0] + i[1] for i in product(("before_", "after_"), WRAPPED)}
