from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repid.asyncapi.models import (
    AsyncAPI3Schema,
    Channel,
    Components,
    Info,
    MessageObject,
    Operation,
    Server,
)

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.worker import Worker


class AsyncAPIGenerator:
    """Generates AsyncAPI schemas from a Repid Worker instance.

    This class analyzes actors, queues, and message structures to create
    a valid AsyncAPI schema document that describes the messaging interface.
    """

    def __init__(
        self,
        worker: Worker,
        *,
        title: str = "Repid API",
        version: str = "0.1.0",
        description: str | None = None,
    ):
        """Initialize the AsyncAPI generator with a Worker instance.

        Args:
            worker: The Worker instance to generate schema from
            title: API title for the AsyncAPI document
            version: API version string
            description: Optional description of the API
        """
        self.worker = worker
        self.title = title
        self.version = version
        self.description = description

    def generate_schema(self) -> AsyncAPI3Schema:
        """Generate a complete AsyncAPI schema document.

        Returns:
            An AsyncAPI3Schema representing the AsyncAPI schema
        """
        # Create Info object
        info: Info = {
            "title": self.title,
            "version": self.version,
        }
        if self.description:
            info["description"] = self.description

        # Create the schema using models
        schema: AsyncAPI3Schema = {
            "asyncapi": "3.0.0",
            "info": info,
            "servers": self._generate_servers(),
            "channels": self._generate_channels(),
            "operations": self._generate_operations(),
            "components": self._generate_components(),
        }

        return schema

    def _generate_servers(self) -> dict[str, Server]:
        """Generate the servers section of the AsyncAPI schema."""
        servers: dict[str, Server] = {}

        # get it from message broker?
        # self.worker._conn.message_broker

        return servers

    def _generate_channels(self) -> dict[str, Channel]:
        """Generate the channels section of the AsyncAPI schema."""
        channels: dict[str, Channel] = {}

        for queue_name, topics in self.worker.topics_by_queue.items():
            for topic in topics:
                actor = self.worker.actors.get(topic)
                if not actor:
                    continue

                channel_key = f"{queue_name}/{topic}"
                channels[channel_key] = {
                    "title": f"{topic} channel",
                    "summary": f"Channel for {topic} messages on queue {queue_name}",
                    "description": self._get_actor_summary(actor),
                    "messages": {topic: {"$ref": f"#/components/messages/{topic}"}},
                }

        return channels

    def _generate_operations(self) -> dict[str, Operation]:
        """Generate the operations section of the AsyncAPI schema."""
        operations: dict[str, Operation] = {}

        for queue_name, topics in self.worker.topics_by_queue.items():
            for topic in topics:
                actor = self.worker.actors.get(topic)
                if not actor:
                    continue

                channel_ref = f"#/channels/{queue_name}/{topic}"

                # Add receive operation (consumer perspective)
                operation_key = f"receive{topic.capitalize()}"
                operations[operation_key] = {
                    "action": "receive",
                    "channel": {"$ref": channel_ref},
                    "summary": f"Receive {topic} messages",
                    "description": self._get_actor_summary(actor),
                    "messages": [{"$ref": f"#/components/messages/{topic}"}],
                }

        return operations

    def _generate_components(self) -> Components:
        """Generate the components section of the AsyncAPI schema."""
        return {
            "messages": self._generate_messages(),
            "schemas": self._generate_json_schemas(),
        }

    def _generate_messages(self) -> dict[str, MessageObject]:
        """Generate the messages section of the AsyncAPI schema."""
        messages: dict[str, MessageObject] = {}

        for topic, actor_data in self.worker.actors.items():
            messages[topic] = {
                "name": topic,
                "title": topic.replace("_", " ").capitalize(),
                "summary": self._get_actor_summary(actor_data),
                "content_type": "application/json",
                "payload": {"$ref": f"#/components/schemas/{topic}Input"},
            }

        return messages

    def _generate_json_schemas(self) -> dict[str, Any]:
        schemas: dict[str, Any] = {}

        for topic, actor_data in self.worker.actors.items():
            schemas[f"{topic}Input"] = actor_data.converter.get_input_schema()
            schemas[f"{topic}Output"] = actor_data.converter.get_output_schema()

        return schemas

    def _get_actor_summary(self, actor_data: ActorData) -> str:
        """Extract a summary from the actor function docstring."""
        fn = actor_data.fn
        if fn.__doc__:
            # Return the first line of the docstring
            return fn.__doc__.strip().split("\n")[0]
        return f"Handler for {actor_data.name} messages"
