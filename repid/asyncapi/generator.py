from __future__ import annotations

import re
from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING

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
from repid.asyncapi.models.common import (
    ChannelBindingsObject,
    CorrelationId,
    ExternalDocs,
    MessageBindingsObject,
    OperationBindingsObject,
    ReferenceModel,
    ServerBindingsObject,
    Tag,
)
from repid.asyncapi.models.info import Contact, License
from repid.connections.abc import ServerT
from repid.data.actor import ActorData
from repid.data.channel import Channel as ChannelDataModel
from repid.data.message_schema import MessageSchema
from repid.data.operation import SendOperation

if TYPE_CHECKING:
    from repid.data.contact import Contact as ContactModel
    from repid.data.converter_input_schema import ConverterInputSchema
    from repid.data.external_docs import ExternalDocs as ExternalDocsModel
    from repid.data.license import License as LicenseModel
    from repid.data.message_schema import MessageExample
    from repid.data.tag import Tag as TagModel
    from repid.message_registry import MessageRegistry
    from repid.router import Router
    from repid.server_registry import ServerRegistry


SecurityScheme = dict[str, object]


class AsyncAPIComponents:
    def __init__(self) -> None:
        self.schemas: dict[str, dict] = {}
        self.messages: dict[str, MessageObject] = {}
        self.security_schemes: dict[str, SecurityScheme] = {}
        self.server_bindings: dict[str, ServerBindingsObject] = {}
        self.channel_bindings: dict[str, ChannelBindingsObject] = {}
        self.operation_bindings: dict[str, OperationBindingsObject] = {}
        self.message_bindings: dict[str, MessageBindingsObject] = {}
        self.tags: dict[str, Tag] = {}
        self.external_docs: dict[str, ExternalDocs] = {}

        self._external_docs_lookup: dict[str, str] = {}

    def add_message(self, message: MessageObject, original_name: str) -> str:
        name = original_name
        counter = 1
        while name in self.messages:
            name = f"{original_name}_{counter}"
            counter += 1
        self.messages[name] = message
        return name

    def add_tag(self, tag: TagModel) -> ReferenceModel:
        tag_dict: Tag = {"name": tag.name}
        if tag.description:
            tag_dict["description"] = tag.description

        if tag.external_docs:
            ext = self.add_external_docs(tag.external_docs)
            if ext:
                tag_dict["externalDocs"] = ext

        if tag.name not in self.tags:
            self.tags[tag.name] = tag_dict

        return {"$ref": f"#/components/tags/{tag.name}"}

    def add_security_scheme(self, name: str, scheme: SecurityScheme) -> ReferenceModel:
        if name not in self.security_schemes:
            self.security_schemes[name] = scheme
        return {"$ref": f"#/components/securitySchemes/{name}"}

    def add_server_binding(self, name: str, binding: ServerBindingsObject) -> ReferenceModel:
        if name not in self.server_bindings:
            self.server_bindings[name] = binding
        return {"$ref": f"#/components/serverBindings/{name}"}

    def add_channel_binding(self, name: str, binding: ChannelBindingsObject) -> ReferenceModel:
        if name not in self.channel_bindings:
            self.channel_bindings[name] = binding
        return {"$ref": f"#/components/channelBindings/{name}"}

    def add_operation_binding(self, name: str, binding: OperationBindingsObject) -> ReferenceModel:
        if name not in self.operation_bindings:
            self.operation_bindings[name] = binding
        return {"$ref": f"#/components/operationBindings/{name}"}

    def add_message_binding(self, name: str, binding: MessageBindingsObject) -> ReferenceModel:
        if name not in self.message_bindings:
            self.message_bindings[name] = binding
        return {"$ref": f"#/components/messageBindings/{name}"}

    def add_external_docs(
        self,
        docs: ExternalDocsModel | ExternalDocs | None,
    ) -> ExternalDocs | ReferenceModel | None:
        if docs is None:
            return None  # pragma: no cover

        url = ""
        description = None

        if isinstance(docs, dict):
            url = docs.get("url", "")
            description = docs.get("description")
        else:
            url = docs.url
            description = docs.description

        if not url:
            return None  # pragma: no cover

        component_name = self._external_docs_lookup.get(url)
        if component_name is None:
            component_name = self._build_external_docs_component_name(url)
            self._external_docs_lookup[url] = component_name

            doc_obj: ExternalDocs = {"url": url}
            if description:
                doc_obj["description"] = description
            self.external_docs[component_name] = doc_obj

        return {"$ref": f"#/components/externalDocs/{component_name}"}

    def _build_external_docs_component_name(self, url: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", url.lower()).strip("_")
        if not slug:
            slug = "external_docs"
        if not slug.startswith("external_docs"):
            slug = f"external_docs_{slug}"
        candidate = slug
        index = 2
        while candidate in self.external_docs:
            candidate = f"{slug}_{index}"
            index += 1
        return candidate

    def extract_definitions(self, schema: dict) -> None:
        if "$defs" in schema:
            for name, definition in schema.pop("$defs").items():
                self.schemas[name] = definition


class DataExtractor:
    def __init__(self, components: AsyncAPIComponents):
        self.components = components

    def extract_common(
        self,
        source: ActorData | ChannelDataModel | MessageSchema | ServerT | SendOperation,
        target: Channel | Operation | MessageObject | Server,
    ) -> None:
        if source.title is not None:
            target["title"] = source.title
        if source.summary is not None:
            target["summary"] = source.summary
        if source.description is not None:
            target["description"] = source.description

        if not isinstance(source, ChannelDataModel) and source.tags:
            tags_list = [self.components.add_tag(t) for t in source.tags if t.name]
            if tags_list:
                target["tags"] = tags_list

        if source.external_docs:
            ext = self.components.add_external_docs(source.external_docs)
            if ext:
                target["externalDocs"] = ext

    def message_from_schema(self, obj: MessageSchema) -> MessageObject:
        msg: MessageObject = {"name": obj.name}
        self.extract_common(obj, msg)

        if obj.content_type is not None:
            msg["contentType"] = obj.content_type
        if obj.headers is not None:
            headers_obj = deepcopy(obj.headers)
            self.components.extract_definitions(headers_obj)
            msg["headers"] = headers_obj
        if obj.payload is not None:
            payload_obj = deepcopy(obj.payload)
            self.components.extract_definitions(payload_obj)
            msg["payload"] = payload_obj

        if obj.correlation_id is not None:
            corr: CorrelationId = {"location": obj.correlation_id.location}
            if obj.correlation_id.description is not None:
                corr["description"] = obj.correlation_id.description
            msg["correlationId"] = corr

        if obj.deprecated:
            msg["deprecated"] = True

        if isinstance(obj.bindings, dict) and obj.bindings:
            binding_name = f"{obj.name}-message_bindings"
            msg["bindings"] = {"$ref": f"#/components/messageBindings/{binding_name}"}
            self.components.add_message_binding(binding_name, obj.bindings)

        self._attach_examples_from_schema(msg, obj)
        return msg

    def message_from_actor(self, actor: ActorData) -> MessageObject:
        msg: MessageObject = {"name": actor.name}
        self.extract_common(actor, msg)

        input_schema = actor.converter.get_input_schema()
        self._apply_input_schema(msg, input_schema)

        if actor.deprecated:
            msg["deprecated"] = True
        return msg

    def _apply_input_schema(
        self,
        msg: MessageObject,
        input_schema: ConverterInputSchema,
    ) -> None:
        if input_schema.payload_schema is not None:
            msg["payload"] = input_schema.payload_schema
            self.components.extract_definitions(msg["payload"])
        if input_schema.content_type is not None:
            msg["contentType"] = input_schema.content_type
        if input_schema.headers_schema is not None:
            msg["headers"] = input_schema.headers_schema
            self.components.extract_definitions(msg["headers"])
        if input_schema.correlation_id is not None:
            corr_obj = input_schema.correlation_id
            corr: CorrelationId = {"location": corr_obj.location}
            if corr_obj.description is not None:
                corr["description"] = corr_obj.description
            msg["correlationId"] = corr

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

    def generate_schema(self) -> AsyncAPI3Schema:  # noqa: C901, PLR0912
        """Generate the AsyncAPI schema."""
        components = AsyncAPIComponents()
        extractor = DataExtractor(components)
        message_keys: dict[int, str] = {}

        # 1. Collect Messages and Bindings
        for router in self.routers:
            for actors in router._actors_per_channel_address.values():
                for actor in actors:
                    actor_msg = extractor.message_from_actor(actor)
                    key = components.add_message(actor_msg, actor.name)
                    message_keys[id(actor)] = key

        for op in self.messages._operations.values():
            for op_msg in op.messages:
                op_msg_obj = extractor.message_from_schema(op_msg)
                key = components.add_message(op_msg_obj, op_msg.name)
                message_keys[id(op_msg)] = key

        # 3. Generate parts that populate components
        info = self._generate_info(components)
        servers = self._generate_servers(extractor, components)
        channels = self._generate_channels(extractor, components, message_keys)
        operations = self._generate_operations(extractor, components, message_keys)

        # 4. Build Components Object
        comps = Components(messages=components.messages)
        if components.schemas:
            comps["schemas"] = components.schemas
        if components.security_schemes:
            comps["securitySchemes"] = components.security_schemes
        if components.server_bindings:
            comps["serverBindings"] = components.server_bindings
        if components.channel_bindings:
            comps["channelBindings"] = components.channel_bindings
        if components.operation_bindings:
            comps["operationBindings"] = components.operation_bindings
        if components.message_bindings:
            comps["messageBindings"] = components.message_bindings
        if components.tags:
            comps["tags"] = components.tags
        if components.external_docs:
            comps["externalDocs"] = components.external_docs

        return AsyncAPI3Schema(
            asyncapi="3.0.0",
            info=info,
            servers=servers,
            channels=channels,
            operations=operations,
            components=comps,
        )

    def _generate_info(self, components: AsyncAPIComponents) -> Info:  # noqa: C901, PLR0912
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
            tags_list = [components.add_tag(t) for t in self.tags if t.name]
            if tags_list:
                info["tags"] = tags_list

        if self.external_docs is not None:
            docs = ExternalDocs(url=self.external_docs.url)
            if self.external_docs.description is not None:
                docs["description"] = self.external_docs.description
            if self.external_docs.url:
                ext_doc = components.add_external_docs(docs)
                if ext_doc:
                    info["externalDocs"] = ext_doc
            else:
                info["externalDocs"] = docs
        return info

    def _generate_servers(
        self,
        extractor: DataExtractor,
        components: AsyncAPIComponents,
    ) -> dict[str, Server]:
        servers: dict[str, Server] = {}
        for name, server in self.servers.list_servers().items():
            srv: Server = {"host": server.host, "protocol": server.protocol}
            extractor.extract_common(server, srv)

            bindings = self._process_server_bindings(server.bindings, name, components)
            if bindings:
                srv["bindings"] = bindings

            if server.protocol_version is not None:
                srv["protocolVersion"] = server.protocol_version
            if server.pathname is not None:
                srv["pathname"] = server.pathname
            if server.variables is not None:
                srv["variables"] = server.variables

            security = self._process_security_schemes(
                server.security,
                name,
                "server",
                components,
            )
            if security:
                srv["security"] = security

            servers[name] = srv
        return servers

    def _generate_channels(  # noqa: C901
        self,
        extractor: DataExtractor,
        components: AsyncAPIComponents,
        message_keys: dict[int, str],
    ) -> dict[str, Channel]:
        channels: dict[str, Channel] = {}
        router_channel_addresses: set[str] = set()

        for router in self.routers:
            actors_by_channel: dict[str, list[ActorData]] = router._actors_per_channel_address
            for ch in router.channels:
                channel_obj: Channel = {}
                extractor.extract_common(ch, channel_obj)
                if isinstance(ch, ChannelDataModel):
                    bindings = self._process_channel_bindings(
                        ch.bindings,
                        ch.address,
                        components,
                    )
                    if bindings:
                        channel_obj["bindings"] = bindings

                messages_map = self._channel_messages_for_address(ch.address, actors_by_channel)
                if messages_map:
                    channel_obj["messages"] = messages_map

                channels[ch.address] = channel_obj
                router_channel_addresses.add(ch.address)

        for op in self.messages._operations.values():
            if op.channel.address not in channels:
                channels[op.channel.address] = {}

            channel_obj = channels[op.channel.address]
            extractor.extract_common(op.channel, channel_obj)

            bindings = self._process_channel_bindings(
                op.channel.bindings,
                op.channel.address,
                components,
            )
            if bindings:
                channel_obj["bindings"] = bindings

            operation_channel = channel_obj
            if op.messages:
                op_ch_messages = {
                    msg.name: ReferenceModel(
                        {
                            "$ref": f"#/components/messages/{message_keys.get(id(msg), msg.name)}",
                        },
                    )
                    for msg in op.messages
                }
                operation_messages: dict[
                    str,
                    ReferenceModel,
                ] = operation_channel.setdefault("messages", {})  # type: ignore[assignment]
                for k, v in op_ch_messages.items():
                    if k not in operation_messages:
                        operation_messages[k] = v
        return channels

    def _channel_messages_for_address(
        self,
        address: str,
        actors_by_channel: dict[str, list[ActorData]],
    ) -> dict[str, ReferenceModel]:
        messages_map: dict[str, ReferenceModel] = {}
        for actor in actors_by_channel.get(address, []):
            messages_map[actor.name] = {"$ref": f"#/components/messages/{actor.name}"}
        for op in self.messages._operations.values():
            if op.channel.address == address:
                for msg in op.messages:
                    messages_map[msg.name] = {"$ref": f"#/components/messages/{msg.name}"}
        return messages_map

    def _generate_operations(
        self,
        extractor: DataExtractor,
        components: AsyncAPIComponents,
        message_keys: dict[int, str],
    ) -> dict[str, Operation]:
        operations: dict[str, Operation] = {}
        for router in self.routers:
            for actor in router.actors:
                operations.update(
                    self._generate_router_operations(
                        actor.channel_address,
                        {actor.name: actor},
                        extractor,
                        components,
                    ),
                )
        for op_id, op_obj in self.messages._operations.items():
            operations[op_id] = self._generate_operation_send(
                op_obj,
                extractor,
                components,
                op_id,
                message_keys,
            )
        return operations

    def _generate_router_operations(
        self,
        channel: str,
        actors: dict[str, ActorData],
        extractor: DataExtractor,
        components: AsyncAPIComponents,
    ) -> dict[str, Operation]:
        result: dict[str, Operation] = {}
        for actor_name, actor in actors.items():
            channel_ref = f"#/channels/{channel.replace('/', '~1')}"
            op: Operation = {
                "action": "receive",
                "channel": {"$ref": channel_ref},
            }
            extractor.extract_common(actor, op)

            op["messages"] = [
                {
                    "$ref": f"#/channels/{channel.replace('/', '~1')}/messages/{actor_name.replace('/', '~1')}",
                },
            ]

            bindings = self._process_operation_bindings(
                actor.bindings,
                actor.name,
                components,
            )
            if bindings:
                op["bindings"] = bindings

            security = self._process_security_schemes(
                actor.security,
                f"receive_{actor_name}",
                "operation",
                components,
            )
            if security:
                op["security"] = security
            result[f"receive_{actor_name}"] = op
        return result

    def _generate_operation_send(
        self,
        op_obj: SendOperation,
        extractor: DataExtractor,
        components: AsyncAPIComponents,
        op_id: str,
        message_keys: dict[int, str],
    ) -> Operation:
        channel_ref = f"#/channels/{op_obj.channel.address.replace('/', '~1')}"
        op: Operation = {
            "action": "send",
            "channel": {"$ref": channel_ref},
        }
        extractor.extract_common(op_obj, op)

        if op_obj.messages:
            escaped_channel = op_obj.channel.address.replace("/", "~1")
            op["messages"] = [
                {
                    "$ref": f"#/channels/{escaped_channel}/messages/{message_keys.get(id(msg), msg.name).replace('/', '~1')}",
                }
                for msg in op_obj.messages
            ]

        bindings = self._process_operation_bindings(
            op_obj.bindings,
            op_id,
            components,
        )
        if bindings:
            op["bindings"] = bindings

        security = self._process_security_schemes(
            op_obj.security,
            op_id,
            "operation",
            components,
        )
        if security:
            op["security"] = security
        return op

    def _process_security_schemes(
        self,
        security: Sequence[SecurityScheme | ReferenceModel] | None,
        parent_id: str,
        parent_type: str,
        components: AsyncAPIComponents,
    ) -> list[SecurityScheme | ReferenceModel] | None:
        if not security:
            return None

        processed_security: list[SecurityScheme | ReferenceModel] = []
        for index, scheme in enumerate(security):
            if isinstance(scheme, dict) and "$ref" not in scheme:
                scheme_name = f"{parent_id}_{parent_type}-security-schema-{index}"
                components.add_security_scheme(scheme_name, scheme)
                processed_security.append(
                    {"$ref": f"#/components/securitySchemes/{scheme_name}"},
                )
            else:  # pragma: no cover
                processed_security.append(scheme)
        return processed_security

    def _process_server_bindings(
        self,
        bindings: ServerBindingsObject | ReferenceModel | None,
        parent_id: str,
        components: AsyncAPIComponents,
    ) -> ReferenceModel | ServerBindingsObject | None:
        if not bindings:
            return None
        if "$ref" in bindings:
            return bindings  # pragma: no cover

        binding_name = f"{parent_id}_bindings"
        return components.add_server_binding(binding_name, bindings)

    def _process_channel_bindings(
        self,
        bindings: ChannelBindingsObject | ReferenceModel | None,
        parent_id: str,
        components: AsyncAPIComponents,
    ) -> ReferenceModel | ChannelBindingsObject | None:
        if not bindings:
            return None
        if "$ref" in bindings:
            return bindings  # pragma: no cover

        binding_name = f"{parent_id}_bindings"
        return components.add_channel_binding(binding_name, bindings)

    def _process_operation_bindings(
        self,
        bindings: OperationBindingsObject | ReferenceModel | None,
        parent_id: str,
        components: AsyncAPIComponents,
    ) -> ReferenceModel | OperationBindingsObject | None:
        if not bindings:
            return None
        if "$ref" in bindings:
            return bindings  # pragma: no cover

        binding_name = f"{parent_id}_bindings"
        return components.add_operation_binding(binding_name, bindings)
