# Protocols

Wire protocol design and message schemas will be documented here, starting in
Phase 11 (networking). Planned topics:

- Realtime plane framing (QUIC/UDP-oriented transport)
- Control plane API (HTTP/gRPC + Protobuf)
- Compact binary serialization format
- Reducer invocation and subscription delta messages
- Id <-> bytes encoding: `nexum-core` ids are `u64` newtypes with no
  serialization yet; Phase 11 must define a stable byte encoding (or serde
  derives) for every id type (`TableId`, `RowId`, `TransactionId`, ...)
