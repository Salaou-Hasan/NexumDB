# Nexum Wire Protocol Specification v1

Version: 1
Status: Stable (implemented in nexum-network/src/protocol.rs)

## Transport

Frames are length-prefixed binary over TCP or WebSocket.

```
+------------------+---------------------------+
| length: u32 LE   | payload (length bytes)    |
+------------------+---------------------------+
```

Maximum payload size is negotiated during handshake (`max_frame_payload`).
All integers are little-endian throughout the protocol.

## Connection lifecycle

1. Client → Server: `Handshake`
2. Server → Client: `HandshakeResponse`
3. Client → Server: `Authenticate`
4. Server → Client: `AuthResult`
5. Client → Server: `AttachWorld`
6. Server → Client: `AttachResult`
7. ... normal operation (subscribe, call_reducer, send_input) ...

## Message encoding

Every message starts with a kind tag (`u16 LE`) followed by fields.
Kinds are sequential within each direction.

### Client → Server messages

| Tag | Kind | Fields |
|-----|------|--------|
| 0 | Handshake | version u16, name str |
| 1 | Authenticate | credentials str |
| 2 | AttachWorld | world WorldId |
| 3 | DetachWorld | — |
| 4 | InputFrame | frame InputFrame |
| 5 | Subscribe | request_id u64, query Query |
| 6 | Unsubscribe | subscription u64 |
| 7 | Resync | subscription u64 |
| 8 | CallReducer | request_id u64, reducer str, args ReducerArgs |
| 9 | Ping | nonce u64 |

Encoding helpers:
- `str`: `[len u32][utf8 bytes]`
- `WorldId`: `u64`
- `Query`: encoded as structured fields (see below)
- `InputFrame`: see below
- `ReducerArgs`: see below

### Server → Client messages

| Tag | Kind | Fields |
|-----|------|--------|
| 0 | HandshakeResponse | version u16, server_name str |
| 1 | AuthResult | ok bool, principal Option<Principal>, error Option<str> |
| 2 | AttachResult | ok bool, world Option<WorldId>, error Option<str> |
| 3 | TickUpdate | world WorldId, tick TickId, tx_id TransactionId, changes Vec<Change>, events Vec<ReducerEvent> |
| 4 | SubscriptionDelta | subscription u64, updates Vec<SubscriptionUpdate> |
| 5 | ReducerResult | request_id u64, ok bool, value Option<Value>, error Option<str> |
| 6 | StaleNotification | subscription u64, seq u64 |
| 7 | ResyncNotification | subscription u64 |
| 8 | Pong | nonce u64 |
| 9 | RateLimitError | request_id u64, bucket u32 |

### Error messages

Errors use tag `19` (rate limit) or generic error codes:

| Code | Meaning |
|------|---------|
| 2 | Unsupported protocol version |
| 17 | Too many commands per frame |
| 19 | Rate limit exceeded |
| 20 | Authentication required |
| 21 | Not attached to a world |
| 22 | Unknown subscription |

## Value encoding

Every value starts with a type tag byte, followed by payload:

| Tag | Type | Payload bytes |
|-----|------|---------------|
| 0 | Bool | 1 |
| 1 | I8 | 1 |
| 2 | I16 | 2 |
| 3 | I32 | 4 |
| 4 | I64 | 8 |
| 5 | U8 | 1 |
| 6 | U16 | 2 |
| 7 | U32 | 4 |
| 8 | U64 | 8 |
| 9 | F32 | 4 |
| 10 | F64 | 8 |
| 11 | String | len u32 + bytes |
| 12 | Bytes | len u32 + bytes |

All numeric payloads are little-endian.

## Row encoding

```
[nvalues u64] [value_0] [value_1] ... [value_{n-1}]
```

Each value uses the Value encoding above.

## RowId encoding

Row ids are `u64`. Provisional row ids (from inserts within a transaction)
have bit 63 set: `0x8000000000000000`. Real (committed) ids never have this bit.

## InputFrame encoding

```
[tick u64]
[command_count u64]
for each command:
  [source u64]     ← gateway stamps authenticated principal id
  [kind_len u32][kind utf8]
  [has_payload u8][payload Value if has_payload == 1]
```

## Query encoding

```
[table_name str]
[predicate_count u64]
for each predicate:
  [column_name str]
  [op u8]          ← Eq=0, Ne=1, Lt=2, Le=3, Gt=4, Ge=5
  [value Value]
[order_column Option<u8 flag + str>]  ← 0 = none, 1 = column follows
[order_dir u8]                           ← 0 = Ascending, 1 = Descending
[limit Option<u8 flag + u64>]
[projection Option<u8 flag + count u64 + col_names...>]
```

## ReducerArgs encoding

```
[arg_count u64]
for each arg (sorted by name):
  [name_len u32][name utf8]
  [value Value]
```

## Change encoding

Each committed change carries table identity and old/new state:

```
[table_id u64]
[row_id u64]
[kind u8]              ← Insert=0, Update=1, Delete=2
Insert: [new_row Row]
Update: [old_row Row][new_row Row]
Delete: [old_row Row]
```

## Subscription update encoding

```
[seq u64]
one of:
  Initial:   [tag=0 u8][rows: count u64 + each (rid u64 + Row)]
  Insert:    [tag=1 u8][seq u64][rid u64][row Row]
  Update:    [tag=2 u8][seq u64][rid u64][row Row]
  Delete:    [tag=3 u8][seq u64][rid u64]
  Stale:     [tag=4 u8][seq u64]
  Resync:    [tag=5 u8][rows: count u64 + each (rid u64 + Row)]
```

## Integrity

Every frame ends with a CRC32 checksum over the payload bytes:

```
[crc32 u32 LE]
```

Checksums are verified on receive; corrupted frames are dropped (never
silently forwarded).

## Determinism guarantees

- Same input commands + same seed = identical committed state
- Reducer call order within a tick = gateway arrival order (FIFO)
- System execution order = (priority, id) ascending
- Cross-partition delivery order = (sent_tick, from_partition, seq)
- Worker count NEVER changes results
