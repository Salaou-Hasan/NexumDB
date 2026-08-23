//! The SDK's protocol surface (ADR-013): thin wrappers over the canonical
//! `nexum_network::protocol` codec.
//!
//! There is exactly **one** wire protocol — the versioned, bounded,
//! checksummed frame format owned by the network crate. The SDK never
//! reimplements serialization; it only maps the codec's errors onto
//! [`SdkError`].

pub use nexum_network::protocol::{
    ClientMessage, DeltaKind, HEADER_LEN, PROTOCOL_MAGIC, PROTOCOL_VERSION, ServerMessage,
};

use crate::error::SdkError;

/// Encodes one client message into a bounded frame.
pub fn encode_client(message: &ClientMessage, max_payload: u32) -> Result<Vec<u8>, SdkError> {
    nexum_network::protocol::encode_client(message, max_payload).map_err(SdkError::from)
}

/// Decodes one server frame into a typed message.
pub fn decode_server(frame: &[u8], max_payload: u32) -> Result<ServerMessage, SdkError> {
    nexum_network::protocol::decode_server(frame, max_payload).map_err(SdkError::from)
}
