# ADR-006 — Reducer execution model

- **Status:** accepted (Phase 6)
- **Phase:** 6 — Reducer API
- **Decision date:** Phase 6

## Context

Nexum has authoritative tables (Phase 2), an in-memory storage engine with row
versions and mutation epochs (Phase 3), an OCC transaction engine with
read-your-writes and conservative phantom protection (Phase 4), and WAL +
snapshots + recovery attached at the `Transaction::commit() -> Vec<Change>`
boundary (Phase 5). Phase 6 introduces the **reducer**: the authoritative
server-side application logic that changes state.

The reducer layer must sit on top of the transaction engine — never beside it —
and must leave clean attachment points for WASM (Phase 7), subscriptions
(Phase 8), and simulation (Phase 9).

## Decisions

### D1. One reducer invocation = one transaction

Every invocation begins a fresh `Transaction`, runs the reducer against a
`ReducerContext` that delegates to it, then either commits or aborts. The
reducer cannot begin, commit, or abort transactions itself. This makes
"reducer" a packaging of application logic around the existing, validated
transaction semantics rather than a new execution model.

### D2. ReducerContext is the only surface; it never exposes `&mut TableStore`

`ReducerContext` wraps `&mut Transaction` + `&TableStore` (shared) and exposes
exactly `get / contains / scan / lookup_unique / insert / update / delete /
emit`. It holds no `&mut TableStore`, so a reducer physically cannot mutate
storage outside the transaction. This is an **API boundary**; native reducers
are trusted code and it is not a sandbox (D9).

### D3. Reducer identity is numeric + symbolic

`ReducerId` (existing `nexum-core` newtype, `u64`) is the stable handle;
`name` is a registry-unique symbol for callers. No version component in Phase 6
(versioned deployment is future work).

### D4. Arguments are a named, deterministic map

`ReducerArgs` is a `BTreeMap<String, Value>`: named (self-documenting),
key-sorted (deterministic iteration), protocol-independent (no HTTP/JSON
coupling), versionable (adding optional keys doesn't break callers), and
serializable via the existing value codec. Typed accessors map missing keys to
`NotFound` and wrong types to `InvalidArgument`.

### D5. Events are transaction-local and die with the transaction

`emit(name, payload)` appends to a buffer inside the context. A successful
commit publishes the buffer (in `emit` order) as part of `ReducerResult`; an
error, conflict, or panic discards it. Event atomicity mirrors write atomicity
— nothing escapes before the transaction succeeds. No global event bus.

### D6. Panics abort via catch_unwind; no rollback machinery

`invoke` runs `execute` inside `std::panic::catch_unwind`. Because writes are
provisional, aborting discards everything with zero authoritative mutation.
The panic surfaces as `Error::Internal("reducer '<name>' panicked")`. Requires
`panic = unwind` (workspace default). No unsafe code.

### D7. `Error::Conflict` is never wrapped

A reducer application error is any `Error` its `execute` returns; an OCC
validation failure is `Error::Conflict` from `commit`. Both surface through
`invoke`'s `Result<ReducerResult, Error>` unchanged, so callers can always
distinguish "rejected by application logic" from "concurrent state changed".
No automatic retry in Phase 6.

### D8. Durability stays outside the reducer

`ReducerResult` carries `tx_id` + `changes`; the runtime (or server) appends to
the WAL. The reducer crate does not depend on `nexum-wal`. Reducer success
means **committed in memory**; durability is decided by the configured WAL
policy (ADR-005 D1).

### D9. Native reducers are trusted code

The API boundary is not a security boundary. The Phase 7 WASM runtime will
provide memory/instruction limits and a restricted host interface. Phase 6
does not design the WASM ABI; WASM reducers will conform to the same semantic
model (same registry, result, error, and event shapes).

### D10. Deterministic by delegation

The context adds nothing nondeterministic: it forwards to the deterministic
transaction/table layers, orders events by `emit`, and the registry lists by
`ReducerId`. No wall-clock time or randomness is exposed to reducers.

### D11. No nested invocation

The context exposes no way to invoke another reducer (or itself). Composition
is ordinary Rust function calls within one reducer's `execute`. Nested
invocations with their own transactions are future work.

## Consequences

**Positive.** Reducers inherit the complete, tested Phase 4 semantics for free
(read-your-writes, version OCC, missing-row observations, epoch phantom
protection, unique-key validation, deterministic commit ordering). The
`Vec<Change>` boundary that WAL already consumes is exactly what
subscriptions (Phase 8) will consume. Panic safety falls out of the
provisional-write model — no rollback code exists. The registry/result/event
shapes are stable inputs to the Phase 7 WASM design.

**Negative / costs.** Native reducers are trusted code — no isolation from
malicious logic in this phase. A panicking reducer is caught only under
`panic = unwind`. Arguments are untyped at the API level (typed accessors
recover errors at call sites); fully typed signatures arrive with the WASM
toolchain. No reducer-to-reducer invocation yet. A reducer's `return_value`
may carry a **provisional** row id if it returns its insert handle — reducers
should return primary keys; the committed `Change` carries the real storage
id (design doc Q22).

**Risks.** The `catch_unwind` boundary must stay exactly at the `execute` call
— a panic anywhere else in the invocation path (e.g. inside commit) is a bug,
not a reducer failure. Tests cover this.

## Alternative considered

- **Reducer as free function over `&mut TableStore`**: rejected — would bypass
  transactions and the authoritative state invariant.
- **Reducer abstraction over `Table` directly**: rejected — duplicates storage
  semantics and would allow non-transactional mutation.
- **Arguments as positional `Vec<Value>`**: rejected — less self-documenting,
  harder to version.
- **Event bus / pub-sub at Phase 6**: rejected — deferred to Phase 8
  (subscriptions) per the phase plan.
