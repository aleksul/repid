# Server Registry

Repid allows you to connect to multiple message brokers simultaneously. To manage these connections, Repid uses a **Server Registry**.

The Server Registry is accessible via the `app.servers` property.

## Registering Servers

When you initialize your Repid application, you typically register at least one server.

```
from repid import Repid, AmqpServer, RedisServer

app = Repid()

# Registering a RabbitMQ server
app.servers.register_server("rabbitmq", AmqpServer("amqp://localhost"), is_default=True)

# Registering a Redis server at the same time
app.servers.register_server("redis", RedisServer("redis://localhost"))
```

When registering a server, you must provide a unique name (e.g., `"rabbitmq"`) and the server instance itself.

If `is_default=True` is provided (or if it is the first server registered), it will be used as the default server across the framework.

## Retrieving Servers

You can access registered servers by their name:

```
# Returns the server registered as "rabbitmq"
server = app.servers.get_server("rabbitmq")
```

You can also use dictionary-style access:

```
server = app.servers["rabbitmq"]
```

## Default Server

You can get the default server by accessing the `default` property:

```
default_server = app.servers.default
```

If you need to change the default server later, you can use the `set_default_server` method:

```
app.servers.set_default_server("redis")
```

## Usage in Routing and Workers

When sending messages or starting workers, if no server name is specified, Repid will automatically fallback to the default server in the registry.

```
# Will use the default server
await app.send_message(channel="tasks", payload=b"...")

# Will explicitly use the "redis" server
await app.send_message(
    channel="tasks",
    payload=b"...",
    server_name="redis"
)
```

Likewise, when running a worker:

```
# Runs a worker consuming from the explicit "redis" server
await app.run_worker(server_name="redis")
```
