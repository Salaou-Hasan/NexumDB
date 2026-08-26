/**
 * Nexum wire protocol types — mirrors docs/protocols/wire-format-v1.md
 *
 * All multi-byte integers are little-endian.
 * All strings are [len u32][utf8 bytes].
 */

// ─── Value tags ──────────────────────────────────────────────────────────

export enum ColumnType {
  Bool = 0,
  I8 = 1,
  I16 = 2,
  I32 = 3,
  I64 = 4,
  U8 = 5,
  U16 = 6,
  U32 = 7,
  U64 = 8,
  F32 = 9,
  F64 = 10,
  String = 11,
  Bytes = 12,
}

// ─── Message kind tags (Client → Server) ────────────────────────────────

export enum ClientMsgKind {
  Handshake = 0,
  Authenticate = 1,
  AttachWorld = 2,
  DetachWorld = 3,
  InputFrame = 4,
  Subscribe = 5,
  Unsubscribe = 6,
  Resync = 7,
  CallReducer = 8,
  Ping = 9,
}

// ─── Message kind tags (Server → Client) ────────────────────────────────

export enum ServerMsgKind {
  HandshakeResponse = 0,
  AuthResult = 1,
  AttachResult = 2,
  TickUpdate = 3,
  SubscriptionDelta = 4,
  ReducerResult = 5,
  StaleNotification = 6,
  ResyncNotification = 7,
  Pong = 8,
}

// ─── Subscription update tags ───────────────────────────────────────────

export enum UpdateTag {
  Initial = 0,
  Insert = 1,
  Update = 2,
  Delete = 3,
  Stale = 4,
  Resync = 5,
}

// ─── Protocol version ───────────────────────────────────────────────────

export const PROTOCOL_VERSION = 1;
