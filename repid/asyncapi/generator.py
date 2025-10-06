from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repid.asyncapi.models import (
    AsyncAPI3Schema,
    Channel,
    Components,
    Info,
    MessageExampleObject,
    MessageObject,
    Operation,
    Server,
)
from repid.asyncapi.models.channels import ChannelParameter
from repid.asyncapi.models.common import (
    ChannelBindingsObject,
    CorrelationId,
    ExternalDocs,
    MessageBindingsObject,
    MessageTrait,
    OperationBindingsObject,
    ServerBindingsObject,
    Tag,
)
from repid.asyncapi.models.info import Contact, License
from repid.asyncapi.models.operations import OperationTrait

if TYPE_CHECKING:
    from repid.data.actor import ActorData
    from repid.data.channel import Channel as ChannelData
    from repid.data.contact import Contact as ContactModel
    from repid.data.converter_input_schema import ConverterInputSchema
    from repid.data.external_docs import ExternalDocs as ExternalDocsModel
    from repid.data.license import License as LicenseModel
    from repid.data.message_schema import MessageExample, MessageSchema
    from repid.data.operation import SendOperation
    from repid.data.tag import Tag as TagModel
    from repid.main import Repid
    from repid.message_registry import MessageRegistry
    from repid.router import Router
    from repid.server_registry import ServerRegistry


class AsyncAPIGenerator:
    """Generates AsyncAPI schemas from Repid app state (routers, servers, messages, etc)."""

    def __init__(
        self,
        *,
        routers: list[Router],
        servers: ServerRegistry,
        messages: MessageRegistry,
        title: str = "Repid API",
        version: str = "0.1.0",
        description: str | None = None,
        terms_of_service: str | None = None,
        contact: ContactModel | None = None,
        license: LicenseModel | None = None,  # noqa: A002
        tags: list[TagModel] | None = None,
        external_docs: ExternalDocsModel | None = None,
    ):
        self.routers = routers
        self.servers = servers
        self.messages = messages
        self.title = title
        self.version = version
        self.description = description
        self.terms_of_service = terms_of_service
        self.contact = contact
        self.license = license
        self.tags = tags or []
        self.external_docs = external_docs

        # Component collections
        self._component_schemas: dict[str, dict] = {}
        # NOTE: Channel parameters are not yet supported
        self._component_parameters: dict[str, ChannelParameter] = {}
        self._component_messages: dict[str, MessageObject] = {}
        self._component_security_schemes: dict[str, Any] = {}
        self._component_server_bindings: dict[str, ServerBindingsObject] = {}
        self._component_channel_bindings: dict[str, ChannelBindingsObject] = {}
        self._component_operation_bindings: dict[str, OperationBindingsObject] = {}
        self._component_message_bindings: dict[str, MessageBindingsObject] = {}
        self._component_operation_traits: dict[str, OperationTrait] = {}
        self._component_message_traits: dict[str, MessageTrait] = {}
        self._component_tags: dict[str, Tag] = {}
        self._component_external_docs: dict[str, ExternalDocs] = {}

    @staticmethod
    def from_app(app: Repid) -> AsyncAPIGenerator:
        """Create generator from a Repid app instance."""
        return AsyncAPIGenerator(
            routers=app.routers,  # type: ignore[attr-defined]
            servers=app.servers,
            messages=app.messages,
            title=app.title,
            version=app.version,
            description=app.description,
        )

    def _deduplicate_tag(self, tag: Tag) -> Tag:
        if tag["name"] not in self._component_tags:
            self._component_tags[tag["name"]] = tag
        return tag

    def _deduplicate_external_docs(self, docs: ExternalDocs) -> ExternalDocs:
        key = docs["url"]
        if key not in self._component_external_docs:
            self._component_external_docs[key] = docs
        return docs

    def _generate_info(self) -> Info:  # noqa: C901
        info = Info(title=self.title, version=self.version)
        if self.description is not None:
            info["description"] = self.description
        if self.terms_of_service is not None:
            info["termsOfService"] = self.terms_of_service
        if self.contact is not None:
            contact = Contact()
            if self.contact.url is not None:
                contact["url"] = self.contact.url
            if self.contact.email is not None:
                contact["email"] = self.contact.email
            if self.contact.name is not None:
                contact["name"] = self.contact.name
            if contact:
                info["contact"] = contact
        if self.license is not None:
            license_obj = License(name=self.license.name)
            if self.license.url is not None:
                license_obj["url"] = self.license.url
            info["license"] = license_obj
        if self.tags:
            info["tags"] = [
                self._deduplicate_tag(
                    Tag(name=tag.name, description=tag.description)
                    if tag.description
                    else Tag(name=tag.name),
                )
                for tag in self.tags
                if tag.name
            ]
        if self.external_docs is not None:
            docs = ExternalDocs(url=self.external_docs.url)
            if self.external_docs.description is not None:
                docs["description"] = self.external_docs.description
            info["externalDocs"] = self._deduplicate_external_docs(docs)
        return info

    def generate_schema(self) -> AsyncAPI3Schema:  # noqa: C901
        """Generate the AsyncAPI schema."""
        self._collect_all_components()
        components = Components(messages=self._component_messages)
        if self._component_schemas:
            components["schemas"] = self._component_schemas
        if self._component_parameters:
            components["parameters"] = self._component_parameters
        if self._component_security_schemes:
            components["securitySchemes"] = self._component_security_schemes
        if self._component_server_bindings:
            components["serverBindings"] = self._component_server_bindings
        if self._component_channel_bindings:
            components["channelBindings"] = self._component_channel_bindings
        if self._component_operation_bindings:
            components["operationBindings"] = self._component_operation_bindings
        if self._component_message_bindings:
            components["messageBindings"] = self._component_message_bindings
        if self._component_operation_traits:
            components["operationTraits"] = self._component_operation_traits
        if self._component_message_traits:
            components["messageTraits"] = self._component_message_traits
        if self._component_tags:
            components["tags"] = self._component_tags
        if self._component_external_docs:
            components["externalDocs"] = self._component_external_docs
        schema: AsyncAPI3Schema = AsyncAPI3Schema(
            asyncapi="3.0.0",
            info=self._generate_info(),
            servers=self._generate_servers(),
            channels=self._generate_channels(),
            operations=self._generate_operations(),
            components=components,
        )
        return schema

    @staticmethod
    def _build_tags(objs: list[TagModel] | tuple[TagModel, ...] | None) -> list[Tag]:
        tags: list[Tag] = []
        if not objs:
            return tags
        for t in objs:
            tag_dict: Tag = {"name": t.name}
            if t.description:
                tag_dict["description"] = t.description
            tags.append(tag_dict)
        return tags

    def _assign_tags(
        self,
        container: Any,
        objs: list[TagModel] | tuple[TagModel, ...] | None,
    ) -> None:
        tags = [self._deduplicate_tag(tag) for tag in self._build_tags(objs)]
        if tags:
            container["tags"] = tags

    def _docs_from_dataclass(self, docs: ExternalDocsModel | None) -> ExternalDocs | None:
        if docs is None or not docs.url:
            return None
        out: ExternalDocs = {"url": docs.url}
        if docs.description:
            out["description"] = docs.description
        return self._deduplicate_external_docs(out)

    def _collect_all_components(self) -> None:
        self._component_messages = {}
        for router in self.routers:
            for actors in router._actors_per_channel_address.values():
                for actor in actors:
                    self._component_messages[actor.name] = self._message_from_actor(actor)
        for op in self.messages._operations.values():
            for msg in op.messages:
                self._component_messages[msg.name] = self._message_from_schema(msg)

        self._collect_schemas()
        self._collect_bindings_and_traits()
        self._collect_security_schemes()

    @staticmethod
    def _set_if(container: Any, key: str, value: Any | None) -> None:
        if value is not None:
            container[key] = value

    def _channel_base_from_dataclass(self, ch: ChannelData) -> Channel:
        out: Channel = {}
        self._set_if(out, "title", ch.title)
        self._set_if(out, "summary", ch.summary)
        self._set_if(out, "description", ch.description)
        if ch.bindings is not None:
            out["bindings"] = ch.bindings
        doc = self._docs_from_dataclass(ch.external_docs)
        if doc is not None:
            out["externalDocs"] = doc
        return out

    def _channel_messages_for_address(
        self,
        address: str,
        actors_by_channel: dict[str, list[ActorData]],
    ) -> dict[str, Any]:
        messages_map: dict[str, Any] = {}
        for actor in actors_by_channel.get(address, []):
            messages_map[actor.name] = {"$ref": f"#/components/messages/{actor.name}"}
        for op in self.messages._operations.values():
            if op.channel.address == address:
                for msg in op.messages:
                    messages_map[msg.name] = {"$ref": f"#/components/messages/{msg.name}"}
        return messages_map

    @staticmethod
    def _merge_channel_missing_fields(base: Channel, extra: Channel) -> None:
        AsyncAPIGenerator._merge_simple_channel_fields(base, extra)
        AsyncAPIGenerator._merge_channel_messages(base, extra)

    @staticmethod
    def _merge_simple_channel_fields(base: Channel, extra: Channel) -> None:
        if "title" in extra and "title" not in base:
            base["title"] = extra["title"]
        if "summary" in extra and "summary" not in base:
            base["summary"] = extra["summary"]
        if "description" in extra and "description" not in base:
            base["description"] = extra["description"]
        if "tags" in extra and "tags" not in base:
            base["tags"] = extra["tags"]
        if "externalDocs" in extra and "externalDocs" not in base:
            base["externalDocs"] = extra["externalDocs"]
        if "bindings" in extra and "bindings" not in base:
            base["bindings"] = extra["bindings"]

    @staticmethod
    def _merge_channel_messages(base: Channel, extra: Channel) -> None:
        if "messages" not in extra:
            return
        if "messages" not in base:
            base["messages"] = extra["messages"]
            return
        for k, v in extra["messages"].items():
            if k not in base["messages"]:
                base["messages"][k] = v  # type: ignore[index]

    def _collect_schemas(self) -> None:
        for _op in self.messages._operations.values():
            for msg in _op.messages:
                if msg.payload:
                    schema_name = f"{msg.name}_payload"
                    self._component_schemas[schema_name] = msg.payload

    def _collect_bindings_and_traits(self) -> None:
        for router in self.routers:
            for actor in router.actors:
                if actor.bindings is not None:
                    binding_name = f"{actor.name}_bindings"
                    self._component_operation_bindings[binding_name] = actor.bindings

    def _collect_security_schemes(self) -> None:
        for _name, server in self.servers.list_servers().items():
            if server.security:
                for scheme in server.security:
                    if isinstance(scheme, dict):
                        for scheme_name, scheme_val in scheme.items():
                            if scheme_name not in self._component_security_schemes:
                                self._component_security_schemes[scheme_name] = scheme_val

    def _generate_servers(self) -> dict[str, Server]:
        servers: dict[str, Server] = {}
        for name, server in self.servers.list_servers().items():
            srv: Server = {"host": server.host, "protocol": server.protocol}
            self._set_if(srv, "title", server.title)
            self._set_if(srv, "description", server.description)
            self._set_if(srv, "bindings", server.bindings)
            self._set_if(srv, "summary", server.summary)
            self._set_if(srv, "protocolVersion", server.protocol_version)
            self._set_if(srv, "pathname", server.pathname)
            self._set_if(srv, "variables", server.variables)
            self._set_if(srv, "security", server.security)
            tags_val = (
                [self._deduplicate_tag(tag) for tag in server.tags if tag["name"]]
                if server.tags is not None
                else None
            )
            self._set_if(srv, "tags", tags_val)
            ext_val = (
                self._deduplicate_external_docs(server.external_docs)
                if server.external_docs is not None
                else None
            )
            self._set_if(srv, "externalDocs", ext_val)
            servers[name] = srv
        return servers

    def _generate_channels(self) -> dict[str, Channel]:
        channels: dict[str, Channel] = {}

        for router in self.routers:
            actors_by_channel: dict[str, list[ActorData]] = router._actors_per_channel_address
            for ch in router.channels:
                channel_obj = self._channel_base_from_dataclass(ch)
                messages_map = self._channel_messages_for_address(ch.address, actors_by_channel)
                self._set_if(channel_obj, "messages", messages_map if messages_map else None)
                channels[ch.address] = channel_obj

        for op in self.messages._operations.values():
            key = op.channel.address
            base = channels.setdefault(key, {})
            self._merge_channel_missing_fields(base, self._generate_operation_channel(op))

        for router in self.routers:
            for addr, actors in router._actors_per_channel_address.items():
                if addr not in channels:
                    channels[addr] = {}
                ch_entry = channels[addr]
                if "messages" not in ch_entry:
                    ch_entry["messages"] = {}
                ch_messages = ch_entry["messages"]
                for actor in actors:
                    if actor.name not in ch_messages:
                        ch_messages[actor.name] = {  # type: ignore[index]
                            "$ref": f"#/components/messages/{actor.name}",
                        }
        return channels

    def _generate_operation_channel(self, op: SendOperation) -> Channel:
        ch: Channel = {}
        if op.title is not None:
            ch["title"] = op.title
        if op.summary is not None:
            ch["summary"] = op.summary
        if op.description is not None:
            ch["description"] = op.description
        if op.messages:
            ch["messages"] = {
                msg.name: {"$ref": f"#/components/messages/{msg.name}"} for msg in op.messages
            }
        self._assign_tags(ch, list(op.tags))
        ext = self._docs_from_dataclass(op.external_docs)
        if ext is not None:
            ch["externalDocs"] = ext
        return ch

    def _generate_operations(self) -> dict[str, Operation]:
        operations: dict[str, Operation] = {}
        for router in self.routers:
            for actor in router.actors:
                operations.update(
                    self._generate_router_operations(actor.channel_address, {actor.name: actor}),
                )
        for op_id, op_obj in self.messages._operations.items():
            operations[op_id] = self._generate_operation_send(op_obj)
        return operations

    def _generate_router_operations(
        self,
        channel: str,
        actors: dict[str, ActorData],
    ) -> dict[str, Operation]:
        result: dict[str, Operation] = {}
        for actor_name, actor in actors.items():
            channel_ref = f"#/channels/{channel.replace('/', '~1')}"
            op: Operation = {
                "action": "receive",
                "channel": {"$ref": channel_ref},
            }
            if actor.title is not None:
                op["title"] = actor.title
            if actor.summary is not None:
                op["summary"] = actor.summary
            if actor.description is not None:
                op["description"] = actor.description
            op["messages"] = [
                {
                    "$ref": f"#/channels/{channel.replace('/', '~1')}/messages/{actor_name.replace('/', '~1')}",
                },
            ]
            if actor.bindings is not None:
                op["bindings"] = actor.bindings
            self._assign_tags(op, list(actor.tags) if actor.tags is not None else None)
            ext = self._docs_from_dataclass(actor.external_docs)
            if ext is not None:
                op["externalDocs"] = ext
            if actor.security is not None:
                op["security"] = actor.security
            result[f"receive_{actor_name}"] = op
        return result

    def _generate_operation_send(self, op_obj: SendOperation) -> Operation:
        channel_ref = f"#/channels/{op_obj.channel.address.replace('/', '~1')}"
        op: Operation = {
            "action": "send",
            "channel": {"$ref": channel_ref},
        }
        if op_obj.title is not None:
            op["title"] = op_obj.title
        if op_obj.summary is not None:
            op["summary"] = op_obj.summary
        if op_obj.description is not None:
            op["description"] = op_obj.description
        if op_obj.messages:
            escaped_channel = op_obj.channel.address.replace("/", "~1")
            op["messages"] = [
                {"$ref": f"#/channels/{escaped_channel}/messages/{msg.name.replace('/', '~1')}"}
                for msg in op_obj.messages
            ]
        self._assign_tags(op, list(op_obj.tags))
        ext = self._docs_from_dataclass(op_obj.external_docs)
        if ext is not None:
            op["externalDocs"] = ext
        if op_obj.bindings is not None:
            op["bindings"] = op_obj.bindings
        if op_obj.security:
            op["security"] = op_obj.security
        return op

    def _message_from_schema(self, obj: MessageSchema) -> MessageObject:  # noqa: C901
        msg: MessageObject = {"name": obj.name}
        if obj.title is not None:
            msg["title"] = obj.title
        if obj.summary is not None:
            msg["summary"] = obj.summary
        if obj.description is not None:
            msg["description"] = obj.description
        if obj.content_type is not None:
            msg["contentType"] = obj.content_type
        if obj.headers is not None:
            msg["headers"] = obj.headers
        if obj.payload is not None:
            msg["payload"] = obj.payload

        if obj.correlation_id is not None:
            corr: CorrelationId = {"location": obj.correlation_id.location}
            if obj.correlation_id.description is not None:
                corr["description"] = obj.correlation_id.description
            msg["correlationId"] = corr

        self._assign_tags(msg, list(obj.tags) if obj.tags is not None else None)
        ext = self._docs_from_dataclass(obj.external_docs)
        if ext is not None:
            msg["externalDocs"] = ext
        if obj.deprecated:
            msg["deprecated"] = True

        self._attach_examples_from_schema(msg, obj)
        return msg

    def _message_from_actor(self, actor: ActorData) -> MessageObject:
        msg: MessageObject = {"name": actor.name}
        if actor.title is not None:
            msg["title"] = actor.title
        if actor.summary is not None:
            msg["summary"] = actor.summary
        if actor.description is not None:
            msg["description"] = actor.description

        input_schema = actor.converter.get_input_schema()
        self._apply_input_schema(msg, input_schema)

        if actor.deprecated:
            msg["deprecated"] = True
        self._assign_tags(msg, list(actor.tags) if actor.tags is not None else None)
        ext = self._docs_from_dataclass(actor.external_docs)
        if ext is not None:
            msg["externalDocs"] = ext
        return msg

    @staticmethod
    def _build_examples_list(
        examples: tuple[MessageExample, ...] | None,
    ) -> list[MessageExampleObject]:
        if not examples:
            return []
        out: list[MessageExampleObject] = []
        for ex in examples:
            ex_dict: MessageExampleObject = {}  # type: ignore[assignment]
            if ex.name is not None:
                ex_dict["name"] = ex.name
            if ex.summary is not None:
                ex_dict["summary"] = ex.summary
            if ex.headers is not None:
                ex_dict["headers"] = ex.headers
            if ex.payload is not None:
                ex_dict["payload"] = ex.payload
            if "headers" in ex_dict or "payload" in ex_dict:
                out.append(ex_dict)
        return out

    def _attach_examples_from_schema(self, msg: MessageObject, obj: MessageSchema) -> None:
        examples_list = self._build_examples_list(obj.examples)
        if examples_list:
            msg["examples"] = examples_list

    @staticmethod
    def _apply_input_schema(msg: MessageObject, input_schema: ConverterInputSchema | None) -> None:
        if input_schema is None:
            return
        if input_schema.payload_schema is not None:
            msg["payload"] = input_schema.payload_schema
        if input_schema.content_type is not None:
            msg["contentType"] = input_schema.content_type
        if input_schema.headers_schema is not None:
            msg["headers"] = input_schema.headers_schema
        if input_schema.correlation_id is not None:
            corr_obj = input_schema.correlation_id
            corr: CorrelationId = {"location": corr_obj.location}
            if corr_obj.description is not None:
                corr["description"] = corr_obj.description
            msg["correlationId"] = corr
