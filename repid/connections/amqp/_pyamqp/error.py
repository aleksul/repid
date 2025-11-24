import contextlib
from enum import Enum

from .amqptypes import AMQPTypes, FieldDefinition
from .constants import FIELD, PORT
from .performatives import Performative


class ErrorCondition(Enum):
    """Shared error conditions

    <type name="amqp-error" class="restricted" source="symbol" provides="error-condition">
        <choice name="internal-error" value="amqp:internal-error"/>
        <choice name="not-found" value="amqp:not-found"/>
        <choice name="unauthorized-access" value="amqp:unauthorized-access"/>
        <choice name="decode-error" value="amqp:decode-error"/>
        <choice name="resource-limit-exceeded" value="amqp:resource-limit-exceeded"/>
        <choice name="not-allowed" value="amqp:not-allowed"/>
        <choice name="invalid-field" value="amqp:invalid-field"/>
        <choice name="not-implemented" value="amqp:not-implemented"/>
        <choice name="resource-locked" value="amqp:resource-locked"/>
        <choice name="precondition-failed" value="amqp:precondition-failed"/>
        <choice name="resource-deleted" value="amqp:resource-deleted"/>
        <choice name="illegal-state" value="amqp:illegal-state"/>
        <choice name="frame-size-too-small" value="amqp:frame-size-too-small"/>
    </type>
    """

    #: An internal error occurred. Operator intervention may be required to resume normaloperation.
    InternalError = b"amqp:internal-error"
    #: A peer attempted to work with a remote entity that does not exist.
    NotFDound = b"amqp:not-found"
    #: A peer attempted to work with a remote entity to which it has no access due tosecurity settings.
    UnauthorizedAccess = b"amqp:unauthorized-access"
    #: Data could not be decoded.
    DecodeError = b"amqp:decode-error"
    #: A peer exceeded its resource allocation.
    ResourceLimitExceeded = b"amqp:resource-limit-exceeded"
    #: The peer tried to use a frame in a manner that is inconsistent with the semantics defined in the specification.
    NotAllowed = b"amqp:not-allowed"
    #: An invalid field was passed in a frame body, and the operation could not proceed.
    InvalidField = b"amqp:invalid-field"
    #: The peer tried to use functionality that is not implemented in its partner.
    NotImplemented = b"amqp:not-implemented"
    #: The client attempted to work with a server entity to which it has no access
    #: because another client is working with it.
    ResourceLocked = b"amqp:resource-locked"
    #: The client made a request that was not allowed because some precondition failed.
    PreconditionFailed = b"amqp:precondition-failed"
    #: A server entity the client is working with has been deleted.
    ResourceDeleted = b"amqp:resource-deleted"
    #: The peer sent a frame that is not permitted in the current state of the Session.
    IllegalState = b"amqp:illegal-state"
    #: The peer cannot send a frame because the smallest encoding of the performative with the currently
    #: valid values would be too large to fit within a frame of the agreed maximum frame size.
    FrameSizeTooSmall = b"amqp:frame-size-too-small"


class ConnectionErrorCondition(Enum):
    """Symbols used to indicate connection error conditions.

    <type name="connection-error" class="restricted" source="symbol" provides="error-condition">
        <choice name="connection-forced" value="amqp:connection:forced"/>
        <choice name="framing-error" value="amqp:connection:framing-error"/>
        <choice name="redirect" value="amqp:connection:redirect"/>
    </type>
    """

    #: An operator intervened to close the Connection for some reason. The client may retry at some later date.
    ConnectionForced = b"amqp:connection:forced"
    #: A valid frame header cannot be formed from the incoming byte stream.
    FramingError = b"amqp:connection:framing-error"
    #: The container is no longer available on the current connection. The peer should attempt reconnection
    #: to the container using the details provided in the info map.
    Redirect = b"amqp:connection:redirect"


class SessionErrorCondition(Enum):
    """Symbols used to indicate session error conditions.

    <type name="session-error" class="restricted" source="symbol" provides="error-condition">
        <choice name="window-violation" value="amqp:session:window-violation"/>
        <choice name="errant-link" value="amqp:session:errant-link"/>
        <choice name="handle-in-use" value="amqp:session:handle-in-use"/>
        <choice name="unattached-handle" value="amqp:session:unattached-handle"/>
    </type>
    """

    #: The peer violated incoming window for the session.
    WindowViolation = b"amqp:session:window-violation"
    #: Input was received for a link that was detached with an error.
    ErrantLink = b"amqp:session:errant-link"
    #: An attach was received using a handle that is already in use for an attached Link.
    HandleInUse = b"amqp:session:handle-in-use"
    #: A frame (other than attach) was received referencing a handle which
    #: is not currently in use of an attached Link.
    UnattachedHandle = b"amqp:session:unattached-handle"


class LinkErrorCondition(Enum):
    """Symbols used to indicate link error conditions.

    <type name="link-error" class="restricted" source="symbol" provides="error-condition">
        <choice name="detach-forced" value="amqp:link:detach-forced"/>
        <choice name="transfer-limit-exceeded" value="amqp:link:transfer-limit-exceeded"/>
        <choice name="message-size-exceeded" value="amqp:link:message-size-exceeded"/>
        <choice name="redirect" value="amqp:link:redirect"/>
        <choice name="stolen" value="amqp:link:stolen"/>
    </type>
    """

    #: An operator intervened to detach for some reason.
    DetachForced = b"amqp:link:detach-forced"
    #: The peer sent more Message transfers than currently allowed on the link.
    TransferLimitExceeded = b"amqp:link:transfer-limit-exceeded"
    #: The peer sent a larger message than is supported on the link.
    MessageSizeExceeded = b"amqp:link:message-size-exceeded"
    #: The address provided cannot be resolved to a terminus at the current container.
    Redirect = b"amqp:link:redirect"
    #: The link has been attached elsewhere, causing the existing attachment to be forcibly closed.
    Stolen = b"amqp:link:stolen"


class AMQPError(Performative):
    """Details of an error.

    :param ~uamqp.ErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    """

    NAME = "ERROR"
    CODE = 0x0000001D
    DEFINITION = (
        FIELD("condition", AMQPTypes.symbol, True, None, False),
        FIELD("description", AMQPTypes.string, False, None, False),
        FIELD("info", FieldDefinition.fields, False, None, False),
    )

    def __new__(
        cls,
        condition: ErrorCondition | None = None,
        description: str | None = None,
        info: dict[bytes, bytes] | None = None,
    ) -> "AMQPError":
        error_instance = super().__new__(cls)
        # if b":link:" in condition:
        #     condition = LinkErrorCondition(condition)
        #     if condition == LinkErrorCondition.Redirect:
        #         return AMQPLinkRedirect.__init__(condition, description=description, info=info)
        #     else:
        #         return AMQPLinkError.__init__(condition, description=description, info=info)
        # elif b":session:" in condition:
        #     condition = SessionErrorCondition(condition)
        #     return AMQPSessionError.__init__(condition, description=description, info=info)
        # elif b":connection:" in condition:
        #     condition = ConnectionErrorCondition(condition)
        #     if condition == ConnectionErrorCondition.Redirect:
        #         return AMQPConnectionRedirect.__init__(condition, description=description, info=info)
        #     else:
        #         return AMQPConnectionError.__init__(condition, description=description, info=info)
        # else:
        with contextlib.suppress(ValueError):
            condition = ErrorCondition(condition)
        error_instance.__init__(  # type: ignore[misc]
            condition=condition,
            description=description,
            info=info,
        )
        return error_instance

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.condition})"  # type: ignore[attr-defined]


class AMQPDecodeError(AMQPError):
    """An error occurred while decoding an incoming frame.

    :param ~uamqp.ErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    """


class AMQPConnectionError(AMQPError):
    """Details of a Connection-level error.

    :param ~uamqp.ConnectionErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    """


class AMQPConnectionRedirect(AMQPConnectionError):  # noqa: N818
    """Details of a Connection-level redirect response.

    The container is no longer available on the current connection.
    The peer should attempt reconnection to the container using the details provided.

    :param ~uamqp.ConnectionErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    :param str hostname: The hostname of the container.
        This is the value that should be supplied in the hostname field of the open frame, and during the SASL and
        TLS negotiation (if used).
    :param str network_host: The DNS hostname or IP address of the machine hosting the container.
    :param int port: The port number on the machine hosting the container.
    """

    def __init__(
        self,
        condition: ConnectionErrorCondition,
        description: str | None = None,
        info: dict[bytes, bytes] | None = None,
    ) -> None:
        info = info or {}
        self.hostname = info.get(b"hostname", b"").decode("utf-8")
        self.network_host = info.get(b"network-host", b"").decode("utf-8")
        self.port = int(info.get(b"port", PORT))
        super().__init__(  # type: ignore[call-arg]
            condition,
            description=description,
            info=info,
        )


class AMQPSessionError(AMQPError):
    """Details of a Session-level error.

    :param ~uamqp.SessionErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    """


class AMQPLinkError(AMQPError):
    """Details of a Link-level error.

    :param ~uamqp.LinkErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    """


class AMQPLinkRedirect(AMQPLinkError):  # noqa: N818
    """Details of a Link-level redirect response.

    The address provided cannot be resolved to a terminus at the current container.
    The supplied information may allow the client to locate and attach to the terminus.

    :param ~uamqp.LinkErrorCondition condition: The error code.
    :param str description: A description of the error.
    :param info: A dictionary of additional data associated with the error.
    :param str hostname: The hostname of the container hosting the terminus.
        This is the value that should be supplied in the hostname field of the open frame, and during SASL
        and TLS negotiation (if used).
    :param str network_host: The DNS hostname or IP address of the machine hosting the container.
    :param int port: The port number on the machine hosting the container.
    :param str address: The address of the terminus at the container.
    """

    def __init__(
        self,
        condition: LinkErrorCondition,
        description: str | None = None,
        info: dict[bytes, bytes] | None = None,
    ) -> None:
        info = info or {}
        self.hostname = info.get(b"hostname", b"").decode("utf-8")
        self.network_host = info.get(b"network-host", b"").decode("utf-8")
        self.port = int(info.get(b"port", PORT))
        self.address = info.get(b"address", b"").decode("utf-8")
        super().__init__(  # type: ignore[call-arg]
            condition,
            description=description,
            info=info,
        )
