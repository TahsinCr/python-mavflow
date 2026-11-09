"""Reusable MAVLink transport, routing, and application protocol tools."""

__version__ = "2.0"

from .application import (
    DEFAULT_APPLICATION_MESSAGE_TYPE,
    DEFAULT_APPLICATION_NETWORK,
    MavApplicationAssembler,
    MavApplicationChannel,
    MavApplicationCodec,
    MavApplicationPacket,
    MavApplicationProtocolError,
)
from .client import MavClient
from .connection import MavConnection, MavUnavailableError
from .contracts import MavHeader, MavMessage
from .dispatch import (
    MavApplicationDispatch,
    MavApplicationDispatcher,
    MavApplicationHandler,
    MavApplicationHandlerRegistry,
    MavApplicationResult,
)
from .endpoint import MavEndpoint
from .message import MavMessageEnvelope
from .peer import (
    MavApplicationPeer,
    MavApplicationPeerState,
    MavApplicationResponse,
)
from .remote_log import (
    REMOTE_LOG_MAX_BATCH_BYTES,
    REMOTE_LOG_MAX_BATCH_RECORDS,
    REMOTE_LOG_MAX_DETAILS_BYTES,
    REMOTE_LOG_PACKET_TYPE,
    REMOTE_LOG_PROTOCOL_VERSION,
    MavRemoteLogBatch,
    MavRemoteLogLevel,
    MavRemoteLogRecord,
)
from .router import MavMessageRouter, MavRouterError, MavRouterStats
from .runtime import MavRuntime, MavRuntimeState

__all__ = [
    "DEFAULT_APPLICATION_MESSAGE_TYPE",
    "DEFAULT_APPLICATION_NETWORK",
    "MavApplicationAssembler",
    "MavApplicationChannel",
    "MavApplicationCodec",
    "MavApplicationDispatch",
    "MavApplicationDispatcher",
    "MavApplicationHandler",
    "MavApplicationHandlerRegistry",
    "MavApplicationPacket",
    "MavApplicationPeer",
    "MavApplicationPeerState",
    "MavApplicationProtocolError",
    "MavApplicationResponse",
    "MavApplicationResult",
    "MavClient",
    "MavConnection",
    "MavEndpoint",
    "MavHeader",
    "MavMessage",
    "MavMessageEnvelope",
    "MavMessageRouter",
    "MavRemoteLogBatch",
    "MavRemoteLogLevel",
    "MavRemoteLogRecord",
    "MavRouterError",
    "MavRouterStats",
    "MavRuntime",
    "MavRuntimeState",
    "MavUnavailableError",
    "REMOTE_LOG_MAX_BATCH_BYTES",
    "REMOTE_LOG_MAX_BATCH_RECORDS",
    "REMOTE_LOG_MAX_DETAILS_BYTES",
    "REMOTE_LOG_PACKET_TYPE",
    "REMOTE_LOG_PROTOCOL_VERSION",
]
