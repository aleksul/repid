# Chaining jobs

Sometimes you want to break a complex task into multiple smaller steps, where the output of one task
triggers the next.

In Repid, you can easily chain tasks by using the `Message` dependency to send a new message
directly from within your actor. Because you are using the same server connection, this is extremely
fast.

## Example

Let's imagine you are designing a user registration pipeline. The first job creates a user ID, and
the second job sends a welcome email using that ID.

```python
import asyncio
from repid import Repid, Router, InMemoryServer, Message

app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

router = Router()

@router.actor(channel="registration")
async def create_user(username: str, message: Message) -> None:
    # 1. Pretend we save to the database and generate an ID
    user_id = 123
    print(f"Created user {username} with ID {user_id}")

    # 2. Chain the next job!
    # We send a message to the email queue, passing along the generated user_id
    await message.send_message_json(
        channel="email",
        payload={"user_id": user_id, "username": username},
        headers={"topic": "send_welcome_email"}
    )

    # The current message is automatically acknowledged when this actor finishes successfully.

@router.actor(channel="email")
async def send_welcome_email(user_id: int, username: str) -> None:
    print(f"Sending welcome email to User #{user_id} ({username})!")

app.include_router(router)

async def main() -> None:
    async with app.servers.default.connection():
        # Kick off the chain by sending the first message
        await app.send_message_json(
            channel="registration",
            payload={"username": "Alex"},
            headers={"topic": "create_user"}
        )

        # Process the queue.
        # It will first process the 'create_user' message,
        # which will then enqueue the 'send_welcome_email' message,
        # which will then be processed in the next iteration!
        await app.run_worker(messages_limit=2)

if __name__ == "__main__":
    asyncio.run(main())
```

When you run this script, the worker will process both jobs in sequence!

```bash
Created user Alex with ID 123
Sending welcome email to User #123 (Alex)!
```

### The `reply_json` helper

If you are designing a strict request-response or RPC-like architecture, you can use the
`message.reply_json()` method. This will automatically publish a message to a reply queue (if
supported and specified by your broker) while simultaneously acknowledging the current message.
