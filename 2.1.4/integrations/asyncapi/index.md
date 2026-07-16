# AsyncAPI Schema & Server

Repid has native support for generating AsyncAPI 3.0 documentation from your actors and routers, and can automatically serve it via an embedded lightweight HTTP server.

## Defining the Application Metadata

When you instantiate your `Repid` application, you can specify metadata about your background task queue:

```
from repid import Repid, Contact, License

app = Repid(
    title="My Repid Task Queue",
    version="1.0.0",
    description="Background task queue processing orders.",
    contact=Contact(name="Support", url="https://example.com", email="support@example.com"),
    license=License(name="MIT", url="https://opensource.org/licenses/MIT"),
)
```

## Generating the Schema

To output the raw AsyncAPI dictionary (which can be serialized and saved as JSON/YAML):

```
schema = app.generate_asyncapi_schema()
```

## Generating the HTML Documentation

If you want to render the AsyncAPI schema as a React UI (for instance, to host it on a custom endpoint via your own web server), you can use the `app.asyncapi_html()` method. It allows you to customize the rendered components.

```
html_content = app.asyncapi_html(
    sidebar=True,
    info=True,
    servers=True,
    operations=True,
    messages=True,
    schemas=True,
    errors=True,
    expand_message_examples=True,
)
```

## Running the Embedded Server

Repid can serve the interactive AsyncAPI React UI alongside your workers natively. To do this, configure the `AsyncAPIServerSettings` when calling `run_worker`:

```
import asyncio
from repid import Repid, AsyncAPIServerSettings, AmqpServer

app = Repid()

# Register your broker to show up as a server in the generated docs
broker = AmqpServer("amqp://localhost")
app.servers.register_server("rabbitmq_default", broker, is_default=True)

async def main():
    async with app.servers.default.connection():
        await app.run_worker(
            asyncapi_server=AsyncAPIServerSettings(
                address="0.0.0.0",
                port=8081,
                endpoint_name="/"
            )
        )

asyncio.run(main())
```

Once started, navigate to `http://localhost:8081/` in your browser to view your AsyncAPI documentation in real-time. This server runs automatically inside a dedicated asyncio task and shares the lifecycle of the worker.
