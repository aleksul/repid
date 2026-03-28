# Routing Strategies (Topics)

By default, Repid uses a **Topic-based routing strategy**. This means a worker reading from a
channel (like `user_tasks`) won't just execute *any* actor. It looks for an actor whose name matches
the `topic` header of the message.

If your actor function is named `send_welcome_email`, the message must be sent with
`headers={"topic": "send_welcome_email"}`.

## Modifying the Topic Name

If you want the actor to listen to a specific topic that differs from its function name, you can set
the `name` parameter in the decorator:

```python
@router.actor(channel="user_tasks", name="email_worker")
async def send_welcome_email() -> None:
    pass
```

Now, messages sent to the `user_tasks` channel must have `headers={"topic": "email_worker"}` for
this actor to process them.

## Catch-all Routing Strategy

If you only have one actor per channel and want it to process *everything* on that channel
regardless of the topic header, you can switch to the **Catch-all routing strategy**:

```python
from repid import catch_all_routing_strategy

@router.actor(
    channel="user_tasks",
    routing_strategy=catch_all_routing_strategy,
)
async def process_everything_on_channel() -> None:
    # This will process any message arriving on the "user_tasks" channel
    pass
```

!!! tip
    Catch-all routing is especially useful when integrating Repid with
    external systems or legacy queues where you cannot enforce specific
    metadata headers on the incoming messages.

## Custom Routing Strategy

If your messaging logic relies on headers other than `topic` to determine routing, you can easily
define your own custom routing strategy!

A routing strategy is simply a factory function that takes the `actor_name` and returns a callable.
That callable takes an incoming `BaseMessageT` and returns a `bool` indicating whether this actor
should process the message.

```python
from repid import BaseMessageT

def my_custom_routing_strategy(*, actor_name: str, **kwargs) -> callable:
    def strategy(message: BaseMessageT) -> bool:
        # Example: route based on a custom "action" header
        if message.headers is None:
            return False
        return message.headers.get("action") == actor_name

    return strategy

@router.actor(
    channel="user_tasks",
    name="send_email_action",
    routing_strategy=my_custom_routing_strategy
)
async def process_something() -> None:
    pass
```

## Routing Priority & Fallbacks

When a worker pulls a message from a channel, it evaluates the routing strategies of all actors
listening on that channel.

- **Multiple Matches**: If multiple actors match a single message, the message is **always routed to
  the first matching actor** (based on the order they were registered to the router).
- **No Matches**: If no actor matches the message, the worker will reject it (returning it to the
  queue). If the same unrouted message is pulled repeatedly, Repid will eventually classify it as a
  "poison message" and negatively acknowledge (`Nack`) it, to prevent it from permanently blocking
  the queue.
