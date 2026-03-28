# Message Registry

In Repid, you can send messages to a raw channel (queue or topic), but you can also decouple your
application logic from physical queue names using **Operations**.

The **Message Registry** is responsible for keeping track of these operations. It is accessible via
the `app.messages` property.

## Registering Operations

An operation is essentially a logical name for an action that targets a specific channel.

You can register an operation like this:

```python
from repid import Repid

app = Repid()

app.messages.register_operation(
    operation_id="send_welcome_email",
    channel="email_queue",
    title="Send Welcome Email",
    description="Publishes a message to send a welcome email to a newly registered user."
)
```

The `register_operation` method accepts the following arguments:

- `operation_id`: A unique identifier for the operation.
- `channel`: The physical channel (or `Channel` object) the message will be routed to.
- *AsyncAPI metadata*: `title`, `summary`, `description`, `messages` (schemas), `security`, `tags`,
  `external_docs`, and `bindings`.

## Using Operations

Once an operation is registered, you can use its `operation_id` instead of the raw `channel` when
sending messages:

```python
await app.send_message_json(
    operation_id="send_welcome_email",
    payload={"user_id": 123},
    headers={"topic": "send_email_actor"}
)
```

When you do this, Repid looks up the operation in the Message Registry and routes the message to the
underlying `channel` (in this case, `"email_queue"`).

!!! note
    You must provide either an `operation_id` or a `channel`
    to `send_message_json` or `send_message`, but never both.

## Retrieving Operations

You can retrieve a registered operation to inspect its details:

```python
operation = app.messages.get_operation("send_welcome_email")

print(operation.channel.address)  # "email_queue"
print(operation.title)            # "Send Welcome Email"
```

## Integration with AsyncAPI

The primary benefit of registering operations with metadata (like `title` and `description`) is that
it heavily enriches your auto-generated **AsyncAPI 3.0** documentation. Repid uses the Message
Registry to build the `operations` section of the AsyncAPI schema.
