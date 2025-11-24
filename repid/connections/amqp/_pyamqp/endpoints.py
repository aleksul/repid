# The messaging layer defines two concrete types (source and target) to be used as the source and target of a
# link. These types are supplied in the source and target fields of the attach frame when establishing or
# resuming link. The source is comprised of an address (which the container of the outgoing Link Endpoint will
# resolve to a Node within that container) coupled with properties which determine:
#
#   - which messages from the sending Node will be sent on the Link
#   - how sending the message affects the state of that message at the sending Node
#   - the behavior of Messages which have been transferred on the Link, but have not yet reached a
#     terminal state at the receiver, when the source is destroyed.

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Annotated, ClassVar

from .amqptypes import AMQPTAnnotation, AMQPTypes, FieldDefinition, ObjDefinition


class TerminusDurability(IntEnum):
    #: No Terminus state is retained durably
    none = 0
    #: Only the existence and configuration of the Terminus is retained durably.
    configuration = 1
    #: In addition to the existence and configuration of the Terminus, the unsettled state for durable
    #: messages is retained durably.
    unsettled_state = 2


class ExpiryPolicy(bytes, Enum):
    #: The expiry timer starts when Terminus is detached.
    link_detach = b"link-detach"
    #: The expiry timer starts when the most recently associated session is ended.
    session_end = b"session-end"
    #: The expiry timer starts when most recently associated connection is closed.
    connection_close = b"connection-close"
    #: The Terminus never expires.
    never = b"never"


class DistributionMode(bytes, Enum):
    #: Once successfully transferred over the link, the message will no longer be available
    #: to other links from the same node.
    move = b"move"
    #: Once successfully transferred over the link, the message is still available for other
    #: links from the same node.
    copy = b"copy"


class LifeTimePolicy(IntEnum):
    #: Lifetime of dynamic node scoped to lifetime of link which caused creation.
    #: A node dynamically created with this lifetime policy will be deleted at the point that the link
    #: which caused its creation ceases to exist.
    delete_on_close = 0x0000002B
    #: Lifetime of dynamic node scoped to existence of links to the node.
    #: A node dynamically created with this lifetime policy will be deleted at the point that there remain
    #: no links for which the node is either the source or target.
    delete_on_no_links = 0x0000002C
    #: Lifetime of dynamic node scoped to existence of messages on the node.
    #: A node dynamically created with this lifetime policy will be deleted at the point that the link which
    #: caused its creation no longer exists and there remain no messages at the node.
    delete_on_no_messages = 0x0000002D
    #: Lifetime of node scoped to existence of messages on or links to the node.
    #: A node dynamically created with this lifetime policy will be deleted at the point that the there are no
    #: links which have this node as their source or target, and there remain no messages at the node.
    delete_on_no_links_or_messages = 0x0000002E


class SupportedOutcomes:
    #: Indicates successful processing at the receiver.
    accepted = b"amqp:accepted:list"
    #: Indicates an invalid and unprocessable message.
    rejected = b"amqp:rejected:list"
    #: Indicates that the message was not (and will not be) processed.
    released = b"amqp:released:list"
    #: Indicates that the message was modified, but not processed.
    modified = b"amqp:modified:list"


class ApacheFilters:
    #: Exact match on subject - analogous to legacy AMQP direct exchange bindings.
    legacy_amqp_direct_binding = b"apache.org:legacy-amqp-direct-binding:string"
    #: Pattern match on subject - analogous to legacy AMQP topic exchange bindings.
    legacy_amqp_topic_binding = b"apache.org:legacy-amqp-topic-binding:string"
    #: Matching on message headers - analogous to legacy AMQP headers exchange bindings.
    legacy_amqp_headers_binding = b"apache.org:legacy-amqp-headers-binding:map"
    #: Filter out messages sent from the same connection as the link is currently associated with.
    no_local_filter = b"apache.org:no-local-filter:list"
    #: SQL-based filtering syntax.
    selector_filter = b"apache.org:selector-filter:string"


@dataclass(slots=True, kw_only=True)
class Source:
    """For containers which do not implement address resolution (and do not admit spontaneous link attachment
    from their partners) but are instead only used as producers of messages, it is unnecessary to provide
    spurious detail on the source. For this purpose it is possible to use a "minimal" source in which all the
    fields are left unset.

    :param str address: The address of the source.
        The address of the source MUST NOT be set when sent on a attach frame sent by the receiving Link Endpoint
        where the dynamic ﬂag is set to true (that is where the receiver is requesting the sender to create an
        addressable node). The address of the source MUST be set when sent on a attach frame sent by the sending
        Link Endpoint where the dynamic ﬂag is set to true (that is where the sender has created an addressable
        node at the request of the receiver and is now communicating the address of that created node).
        The generated name of the address SHOULD include the link name and the container-id of the remote container
        to allow for ease of identification.
    :param ~uamqp.endpoints.TerminusDurability durable: Indicates the durability of the terminus.
        Indicates what state of the terminus will be retained durably: the state of durable messages, only
        existence and configuration of the terminus, or no state at all.
    :param ~uamqp.endpoints.ExpiryPolicy expiry_policy: The expiry policy of the Source.
        Determines when the expiry timer of a Terminus starts counting down from the timeout value. If the link
        is subsequently re-attached before the Terminus is expired, then the count down is aborted. If the
        conditions for the terminus-expiry-policy are subsequently re-met, the expiry timer restarts from its
        originally configured timeout value.
    :param int timeout: Duration that an expiring Source will be retained in seconds.
        The Source starts expiring as indicated by the expiry-policy.
    :param bool dynamic: Request dynamic creation of a remote Node.
        When set to true by the receiving Link endpoint, this field constitutes a request for the sending peer
        to dynamically create a Node at the source. In this case the address field MUST NOT be set. When set to
        true by the sending Link Endpoint this field indicates creation of a dynamically created Node. In this case
        the address field will contain the address of the created Node. The generated address SHOULD include the
        Link name and Session-name or client-id in some recognizable form for ease of traceability.
    :param dict dynamic_node_properties: Properties of the dynamically created Node.
        If the dynamic field is not set to true this field must be left unset. When set by the receiving Link
        endpoint, this field contains the desired properties of the Node the receiver wishes to be created. When
        set by the sending Link endpoint this field contains the actual properties of the dynamically created node.
    :param uamqp.endpoints.DistributionMode distribution_mode: The distribution mode of the Link.
        This field MUST be set by the sending end of the Link if the endpoint supports more than one
        distribution-mode. This field MAY be set by the receiving end of the Link to indicate a preference when a
        Node supports multiple distribution modes.
    :param dict filters: A set of predicates to filter the Messages admitted onto the Link.
        The receiving endpoint sets its desired filter, the sending endpoint sets the filter actually in place
        (including any filters defaulted at the node). The receiving endpoint MUST check that the filter in place
        meets its needs and take responsibility for detaching if it does not.
         Common filter types, along with the capabilities they are associated with are registered
         here: http://www.amqp.org/specification/1.0/filters.
    :param ~uamqp.outcomes.DeliveryState default_outcome: Default outcome for unsettled transfers.
        Indicates the outcome to be used for transfers that have not reached a terminal state at the receiver
        when the transfer is settled, including when the Source is destroyed. The value MUST be a valid
        outcome (e.g. Released or Rejected).
    :param list(bytes) outcomes: Descriptors for the outcomes that can be chosen on this link.
        The values in this field are the symbolic descriptors of the outcomes that can be chosen on this link.
        This field MAY be empty, indicating that the default-outcome will be assumed for all message transfers
        (if the default-outcome is not set, and no outcomes are provided, then the accepted outcome must be
        supported by the source). When present, the values MUST be a symbolic descriptor of a valid outcome,
        e.g. "amqp:accepted:list".
    :param list(bytes) capabilities: The extension capabilities the sender supports/desires.
        See http://www.amqp.org/specification/1.0/source-capabilities.
    """

    # mimics perfomatives for the purposes of encoding
    CODE: ClassVar[int] = 0x00000028
    FRAME_TYPE: ClassVar[bytes] = b"\x00"
    FRAME_OFFSET: ClassVar[bytes] = b"\x02"

    address: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    durable: Annotated[TerminusDurability, AMQPTAnnotation(FieldDefinition.terminus_durability)] = (
        TerminusDurability.none
    )
    expiry_policy: Annotated[ExpiryPolicy, AMQPTAnnotation(FieldDefinition.expiry_policy)] = (
        ExpiryPolicy.session_end
    )
    timeout: Annotated[int, AMQPTAnnotation(FieldDefinition.seconds)] = 0
    dynamic: Annotated[bool, AMQPTAnnotation(AMQPTypes.boolean)] = False
    dynamic_node_properties: Annotated[
        dict | None,
        AMQPTAnnotation(FieldDefinition.node_properties),
    ] = None
    distribution_mode: Annotated[
        DistributionMode | None,
        AMQPTAnnotation(FieldDefinition.distribution_mode),
    ] = None
    filters: Annotated[dict | None, AMQPTAnnotation(FieldDefinition.filter_set)] = None
    default_outcome: Annotated[dict | None, AMQPTAnnotation(ObjDefinition.delivery_state)] = None
    outcomes: Annotated[list[str] | None, AMQPTAnnotation(AMQPTypes.symbol)] = None
    capabilities: Annotated[list[str] | None, AMQPTAnnotation(AMQPTypes.symbol)] = None


@dataclass(slots=True, kw_only=True)
class Target:
    """For containers which do not implement address resolution (and do not admit spontaneous link attachment
    from their partners) but are instead only used as consumers of messages, it is unnecessary to provide spurious
    detail on the source. For this purpose it is possible to use a 'minimal' target in which all the
    fields are left unset.

    :param str address: The address of the source.
        The address of the source MUST NOT be set when sent on a attach frame sent by the receiving Link Endpoint
        where the dynamic ﬂag is set to true (that is where the receiver is requesting the sender to create an
        addressable node). The address of the source MUST be set when sent on a attach frame sent by the sending
        Link Endpoint where the dynamic ﬂag is set to true (that is where the sender has created an addressable
        node at the request of the receiver and is now communicating the address of that created node).
        The generated name of the address SHOULD include the link name and the container-id of the remote container
        to allow for ease of identification.
    :param ~uamqp.endpoints.TerminusDurability durable: Indicates the durability of the terminus.
        Indicates what state of the terminus will be retained durably: the state of durable messages, only
        existence and configuration of the terminus, or no state at all.
    :param ~uamqp.endpoints.ExpiryPolicy expiry_policy: The expiry policy of the Source.
        Determines when the expiry timer of a Terminus starts counting down from the timeout value. If the link
        is subsequently re-attached before the Terminus is expired, then the count down is aborted. If the
        conditions for the terminus-expiry-policy are subsequently re-met, the expiry timer restarts from its
        originally configured timeout value.
    :param int timeout: Duration that an expiring Source will be retained in seconds.
        The Source starts expiring as indicated by the expiry-policy.
    :param bool dynamic: Request dynamic creation of a remote Node.
        When set to true by the receiving Link endpoint, this field constitutes a request for the sending peer
        to dynamically create a Node at the source. In this case the address field MUST NOT be set. When set to
        true by the sending Link Endpoint this field indicates creation of a dynamically created Node. In this case
        the address field will contain the address of the created Node. The generated address SHOULD include the
        Link name and Session-name or client-id in some recognizable form for ease of traceability.
    :param dict dynamic_node_properties: Properties of the dynamically created Node.
        If the dynamic field is not set to true this field must be left unset. When set by the receiving Link
        endpoint, this field contains the desired properties of the Node the receiver wishes to be created. When
        set by the sending Link endpoint this field contains the actual properties of the dynamically created node.
    :param list(bytes) capabilities: The extension capabilities the sender supports/desires.
        See http://www.amqp.org/specification/1.0/source-capabilities.
    """

    # mimics perfomatives for the purposes of encoding
    CODE: ClassVar[int] = 0x00000029
    FRAME_TYPE: ClassVar[bytes] = b"\x00"
    FRAME_OFFSET: ClassVar[bytes] = b"\x02"

    address: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    durable: Annotated[TerminusDurability, AMQPTAnnotation(FieldDefinition.terminus_durability)] = (
        TerminusDurability.none
    )
    expiry_policy: Annotated[ExpiryPolicy, AMQPTAnnotation(FieldDefinition.expiry_policy)] = (
        ExpiryPolicy.session_end
    )
    timeout: Annotated[int, AMQPTAnnotation(FieldDefinition.seconds)] = 0
    dynamic: Annotated[bool, AMQPTAnnotation(AMQPTypes.boolean)] = False
    dynamic_node_properties: Annotated[
        dict | None,
        AMQPTAnnotation(FieldDefinition.node_properties),
    ] = None
    capabilities: Annotated[list[str] | None, AMQPTAnnotation(AMQPTypes.symbol)] = None
