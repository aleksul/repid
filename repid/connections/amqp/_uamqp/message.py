from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, ClassVar

from .amqptypes import AMQPTAnnotation, AMQPTypes, FieldDefinition


class MessageBodyType(Enum):
    DATA = 0
    SEQUENCE = 1
    VALUE = 2
    EMPTY = 3


@dataclass(slots=True, kw_only=True)
class Header:
    """Transport headers for a Message.

    The header section carries standard delivery details about the transfer of a Message through the AMQP
    network. If the header section is omitted the receiver MUST assume the appropriate default values for
    the fields within the header unless other target or node specific defaults have otherwise been set.

    :param bool durable: Specify durability requirements.
        Durable Messages MUST NOT be lost even if an intermediary is unexpectedly terminated and restarted.
        A target which is not capable of fulfilling this guarantee MUST NOT accept messages where the durable
        header is set to true: if the source allows the rejected outcome then the message should be rejected
        with the precondition-failed error, otherwise the link must be detached by the receiver with the same error.
    :param int priority: Relative Message priority.
        This field contains the relative Message priority. Higher numbers indicate higher priority Messages.
        Messages with higher priorities MAY be delivered before those with lower priorities. An AMQP intermediary
        implementing distinct priority levels MUST do so in the following manner:

            - If n distinct priorities are implemented and n is less than 10 - priorities 0 to (5 - ceiling(n/2))
              MUST be treated equivalently and MUST be the lowest effective priority. The priorities (4 + ﬂoor(n/2))
              and above MUST be treated equivalently and MUST be the highest effective priority. The priorities
              (5 ceiling(n/2)) to (4 + ﬂoor(n/2)) inclusive MUST be treated as distinct priorities.
            - If n distinct priorities are implemented and n is 10 or greater - priorities 0 to (n - 1) MUST be
              distinct, and priorities n and above MUST be equivalent to priority (n - 1). Thus, for example, if 2
              distinct priorities are implemented, then levels 0 to 4 are equivalent, and levels 5 to 9 are equivalent
              and levels 4 and 5 are distinct. If 3 distinct priorities are implements the 0 to 3 are equivalent,
              5 to 9 are equivalent and 3, 4 and 5 are distinct. This scheme ensures that if two priorities are distinct
              for a server which implements m separate priority levels they are also distinct for a server which
              implements n diﬀerent priority levels where n > m.

    :param int ttl: Time to live in ms.
        Duration in milliseconds for which the Message should be considered 'live'. If this is set then a message
        expiration time will be computed based on the time of arrival at an intermediary. Messages that live longer
        than their expiration time will be discarded (or dead lettered). When a message is transmitted by an
        intermediary that was received with a ttl, the transmitted message's header should contain a ttl that is
        computed as the difference between the current time and the formerly computed message expiration
        time, i.e. the reduced ttl, so that messages will eventually die if they end up in a delivery loop.
    :param bool first_acquirer: If this value is true, then this message has not been acquired by any other Link.
        If this value is false, then this message may have previously been acquired by another Link or Links.
    :param int delivery_count: The number of prior unsuccessful delivery attempts.
        The number of unsuccessful previous attempts to deliver this message. If this value is non-zero it may
        be taken as an indication that the delivery may be a duplicate. On first delivery, the value is zero.
        It is incremented upon an outcome being settled at the sender, according to rules defined for each outcome.
    """

    # mimics perfomatives for the purposes of encoding
    CODE: ClassVar[int] = 0x00000070
    FRAME_TYPE: ClassVar[bytes] = b"\x00"
    FRAME_OFFSET: ClassVar[bytes] = b"\x02"

    durable: Annotated[bool, AMQPTAnnotation(AMQPTypes.boolean)] = False
    priority: Annotated[int, AMQPTAnnotation(AMQPTypes.ubyte)] = 4
    ttl: Annotated[int | None, AMQPTAnnotation(FieldDefinition.milliseconds)] = None
    first_acquirer: Annotated[bool, AMQPTAnnotation(AMQPTypes.boolean)] = False
    delivery_count: Annotated[int, AMQPTAnnotation(AMQPTypes.uint)] = 0


@dataclass(slots=True, kw_only=True)
class Properties:
    """Immutable properties of the Message.

    The properties section is used for a defined set of standard properties of the message. The properties
    section is part of the bare message and thus must, if retransmitted by an intermediary, remain completely
    unaltered.

    :param message_id: Application Message identifier.
        Message-id is an optional property which uniquely identifies a Message within the Message system.
        The Message producer is usually responsible for setting the message-id in such a way that it is assured
        to be globally unique. A broker MAY discard a Message as a duplicate if the value of the message-id
        matches that of a previously received Message sent to the same Node.
    :param bytes user_id: Creating user id.
        The identity of the user responsible for producing the Message. The client sets this value, and it MAY
        be authenticated by intermediaries.
    :param to: The address of the Node the Message is destined for.
        The to field identifies the Node that is the intended destination of the Message. On any given transfer
        this may not be the Node at the receiving end of the Link.
    :param str subject: The subject of the message.
        A common field for summary information about the Message content and purpose.
    :param reply_to: The Node to send replies to.
        The address of the Node to send replies to.
    :param correlation_id: Application correlation identifier.
        This is a client-specific id that may be used to mark or identify Messages between clients.
    :param bytes content_type: MIME content type.
        The RFC-2046 MIME type for the Message's application-data section (body). As per RFC-2046 this may contain
        a charset parameter defining the character encoding used: e.g. 'text/plain; charset="utf-8"'.
        For clarity, the correct MIME type for a truly opaque binary section is application/octet-stream.
        When using an application-data section with a section code other than data, contenttype, if set, SHOULD
        be set to a MIME type of message/x-amqp+?, where '?' is either data, map or list.
    :param bytes content_encoding: MIME content type.
        The Content-Encoding property is used as a modifier to the content-type. When present, its value indicates
        what additional content encodings have been applied to the application-data, and thus what decoding
        mechanisms must be applied in order to obtain the media-type referenced by the content-type header field.
        Content-Encoding is primarily used to allow a document to be compressed without losing the identity of
        its underlying content type. Content Encodings are to be interpreted as per Section 3.5 of RFC 2616.
        Valid Content Encodings are registered at IANA as "Hypertext Transfer Protocol (HTTP) Parameters"
        (http://www.iana.org/assignments/http-parameters/httpparameters.xml). Content-Encoding MUST not be set when
        the application-data section is other than data. Implementations MUST NOT use the identity encoding.
        Instead, implementations should not set this property. Implementations SHOULD NOT use the compress
        encoding, except as to remain compatible with messages originally sent with other protocols,
        e.g. HTTP or SMTP. Implementations SHOULD NOT specify multiple content encoding values except as to be
        compatible with messages originally sent with other protocols, e.g. HTTP or SMTP.
    :param datetime absolute_expiry_time: The time when this message is considered expired.
        An absolute time when this message is considered to be expired.
    :param datetime creation_time: The time when this message was created.
        An absolute time when this message was created.
    :param str group_id: The group this message belongs to.
        Identifies the group the message belongs to.
    :param int group_sequence: The sequence-no of this message within its group.
        The relative position of this message within its group.
    :param str reply_to_group_id: The group the reply message belongs to.
        This is a client-specific id that is used so that client can send replies to this message to a specific group.
    """

    # mimics perfomatives for the purposes of encoding
    CODE: ClassVar[int] = 0x00000073
    FRAME_TYPE: ClassVar[bytes] = b"\x00"
    FRAME_OFFSET: ClassVar[bytes] = b"\x02"

    message_id: Annotated[
        str | bytes | int | float | None,
        AMQPTAnnotation(FieldDefinition.message_id),
    ] = None
    user_id: Annotated[bytes | None, AMQPTAnnotation(AMQPTypes.binary)] = None
    to: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    subject: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    reply_to: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    correlation_id: Annotated[
        str | bytes | int | float | None,
        AMQPTAnnotation(FieldDefinition.message_id),
    ] = None
    content_type: Annotated[str | None, AMQPTAnnotation(AMQPTypes.symbol)] = None
    content_encoding: Annotated[str | None, AMQPTAnnotation(AMQPTypes.symbol)] = None
    absolute_expiry_time: Annotated[int | datetime | None, AMQPTAnnotation(AMQPTypes.timestamp)] = (
        None
    )
    creation_time: Annotated[int | datetime | None, AMQPTAnnotation(AMQPTypes.timestamp)] = None
    group_id: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None
    group_sequence: Annotated[int | None, AMQPTAnnotation(FieldDefinition.sequence_no)] = None
    reply_to_group_id: Annotated[str | None, AMQPTAnnotation(AMQPTypes.string)] = None


@dataclass(slots=True, kw_only=True)
class Message:
    """An annotated message consists of the bare message plus sections for annotation at the head and tail
    of the bare message.

    There are two classes of annotations: annotations that travel with the message indefinitely, and
    annotations that are consumed by the next node.
    The exact structure of a message, together with its encoding, is defined by the message format. This document
    defines the structure and semantics of message format 0 (MESSAGE-FORMAT). Altogether a message consists of the
    following sections:

        - Zero or one header.
        - Zero or one delivery-annotations.
        - Zero or one message-annotations.
        - Zero or one properties.
        - Zero or one application-properties.
        - The body consists of either: one or more data sections, one or more amqp-sequence sections,
          or a single amqp-value section.
        - Zero or one footer.

    :param ~uamqp.message.Header header: Transport headers for a Message.
        The header section carries standard delivery details about the transfer of a Message through the AMQP
        network. If the header section is omitted the receiver MUST assume the appropriate default values for
        the fields within the header unless other target or node specific defaults have otherwise been set.
    :param dict delivery_annotations: The delivery-annotations section is used for delivery-specific non-standard
        properties at the head of the message. Delivery annotations convey information from the sending peer to
        the receiving peer. If the recipient does not understand the annotation it cannot be acted upon and its
        effects (such as any implied propagation) cannot be acted upon. Annotations may be specific to one
        implementation, or common to multiple implementations. The capabilities negotiated on link attach and on
        the source and target should be used to establish which annotations a peer supports. A registry of defined
        annotations and their meanings can be found here: http://www.amqp.org/specification/1.0/delivery-annotations.
        If the delivery-annotations section is omitted, it is equivalent to a delivery-annotations section
        containing an empty map of annotations.
    :param dict message_annotations: The message-annotations section is used for properties of the message which
        are aimed at the infrastructure and should be propagated across every delivery step. Message annotations
        convey information about the message. Intermediaries MUST propagate the annotations unless the annotations
        are explicitly augmented or modified (e.g. by the use of the modified outcome).
        The capabilities negotiated on link attach and on the source and target may be used to establish which
        annotations a peer understands, however it a network of AMQP intermediaries it may not be possible to know
        if every intermediary will understand the annotation. Note that for some annotation it may not be necessary
        for the intermediary to understand their purpose - they may be being used purely as an attribute which can be
        filtered on. A registry of defined annotations and their meanings can be found here:
        http://www.amqp.org/specification/1.0/message-annotations. If the message-annotations section is omitted,
        it is equivalent to a message-annotations section containing an empty map of annotations.
    :param ~uamqp.message.Properties: Immutable properties of the Message.
        The properties section is used for a defined set of standard properties of the message. The properties
        section is part of the bare message and thus must, if retransmitted by an intermediary, remain completely
        unaltered.
    :param dict application_properties: The application-properties section is a part of the bare message used
        for structured application data. Intermediaries may use the data within this structure for the purposes
        of filtering or routing. The keys of this map are restricted to be of type string (which excludes the
        possibility of a null key) and the values are restricted to be of simple types only (that is excluding
        map, list, and array types).
    :param list(bytes) data_body: A data section contains opaque binary data.
    :param list sequence_body: A sequence section contains an arbitrary number of structured data elements.
    :param value_body: An amqp-value section contains a single AMQP value.
    :param dict footer: Transport footers for a Message.
        The footer section is used for details about the message or delivery which can only be calculated or
        evaluated once the whole bare message has been constructed or seen (for example message hashes, HMACs,
        signatures and encryption details). A registry of defined footers and their meanings can be found
        here: http://www.amqp.org/specification/1.0/footer.
    """

    FORMAT: ClassVar[int] = 0

    header: Header | None = None
    delivery_annotations: dict[str, Any] | None = None
    message_annotations: dict[str, Any] | None = None
    properties: Properties | None = None
    application_properties: dict[str, Any] | None = None
    data: bytes | None = None
    sequence: list | None = None
    value: Any | None = None
    footer: dict[str, Any] | None = None

    @property
    def body(self) -> bytes | list | Any | None:
        if self.data is not None:
            return self.data
        if self.sequence is not None:
            return self.sequence
        if self.value is not None:
            return self.value
        return None

    @property
    def body_type(self) -> MessageBodyType:
        if self.data is not None:
            return MessageBodyType.DATA
        if self.sequence is not None:
            return MessageBodyType.SEQUENCE
        if self.value is not None:
            return MessageBodyType.VALUE
        return MessageBodyType.EMPTY
