#: The IANA assigned port number for AMQP.The standard AMQP port number that has been assigned by IANA
#: for TCP, UDP, and SCTP.There are currently no UDP or SCTP mappings defined for AMQP.
#: The port number is reserved for future transport mappings to these protocols.
PORT = 5672

MAJOR = 1  #: Major protocol version.
MINOR = 0  #: Minor protocol version.
REVISION = 0  #: Protocol revision.

#: The lower bound for the agreed maximum frame size (in bytes). During the initial Connection negotiation, the
#: two peers must agree upon a maximum frame size. This constant defines the minimum value to which the maximum
#: frame size can be set. By defining this value, the peers can guarantee that they can send frames of up to this
#: size until they have agreed a definitive maximum frame size for that Connection.
MIN_MAX_FRAME_SIZE = 512
MAX_FRAME_SIZE_BYTES = 1024 * 1024
MAX_CHANNELS = 65535

#: Lower and upper bounds for a signed 32-bit integer. This is used to be able to differentiate between 32-bit int
#: and 64-bit long values. It is ambiguous at encoding time when using type-checking alone to determine how to
#: encode the value because python 3 does not differentiate between int and long types.
INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647

MIN_MAX_LIST_SIZE = 255
