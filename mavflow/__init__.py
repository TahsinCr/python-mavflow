"""Reusable MAVLink transport, routing, and application protocol tools."""

__version__ = "1.0.0"

from .application import (
    DEFAULT_APPLICATION_MESSAGE_TYPE,
    DEFAULT_APPLICATION_NETWORK,
    MavApplicationAssembler,
    MavApplicationChannel,
    MavApplicationCodec,
    MavApplicationPacket,
    MavApplicationProtocolError,
)
from .cache import MessageCache
from .channel import MavAsyncChannel
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
from .filter import MavMessageFilter
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
from .runtime import MavRuntime, MavRuntimeError, MavRuntimeState

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
    "MavAsyncChannel",
    "MavClient",
    "MavConnection",
    "MavEndpoint",
    "MavHeader",
    "MavMessage",
    "MavMessageEnvelope",
    "MavMessageFilter",
    "MavMessageRouter",
    "MavRemoteLogBatch",
    "MavRemoteLogLevel",
    "MavRemoteLogRecord",
    "MavRouterError",
    "MavRouterStats",
    "MavRuntime",
    "MavRuntimeError",
    "MavRuntimeState",
    "MavUnavailableError",
    "MessageCache",
    "REMOTE_LOG_MAX_BATCH_BYTES",
    "REMOTE_LOG_MAX_BATCH_RECORDS",
    "REMOTE_LOG_MAX_DETAILS_BYTES",
    "REMOTE_LOG_PACKET_TYPE",
    "REMOTE_LOG_PROTOCOL_VERSION",
]
