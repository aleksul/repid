import re

JOB_PREFIX = "job:"
RESULT_PREFIX = "result:"
QUEUE_PREFIX = "queue:"
QUEUE_DEFER_PREFIX = "queue_defer:"

#: This re shows valid actor and queue names.
VALID_NAME_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9._-]*")
