# ADR-007 — WASM reducer runtime

- **Status:** accepted (Phase 7)
- **Phase:** 7 — WASM reducer runtime
- **Decision date:** Phase 7

## Context

Phase 6 established native Rust reducers over `ReducerContext` → `Transaction` →
OCC → `Vec<Change>`. Phase 7 must sandbox **untrusted** reducer programs behind
a restricted host interface while preserving, exactly, the Phase 4 transaction
semantics and the Phase 5 WAL boundary — and without creating a second storage,
transaction, or OCC implementation.

## Decisions

### D1. Engine: wasmi 0.38 (pure-Rust interpreter)

Chosen over wasmtime/wasmer: deterministic interpreter execution,
first-class fuel metering, host-controlled memory limits via `ResourceLimiter`,
low startup cost, small pure-Rust dependency tree, no `'static` bound on
`Store<T>` host data (the host state can borrow the invocation's
`ReducerContext` directly), and no WASI surface unless explicitly added (we
never add it). Latest stable 0.x; the 2.0 line is still beta.

### D2. One import: `("nexum", "op")`

The entire host ABI is a single imported function with opcodes (§4.3 of the
design doc). Module registration **rejects any import other than it**, so
filesystem/network/environment/clock/randomness access is impossible by
construction — the linker defines nothing else. Data crosses the boundary only
as bytes in the guest's linear memory, read/written with wasmi bounds checks.

### D3. Explicit module contract

A valid module exports `memory`, immutable globals `_nexum_in_ptr` /
`_nexum_out_ptr`, and `_nexum_reducer_run() -> i32`. Registration validates
imports, exports, and that the `ABI_IN_CAP`/`ABI_OUT_CAP` buffer regions lie
inside the declared memory. Compiled modules are cached in the registry.

### D4. Envelope protocol

Host ops write `[status: u32][payload_len: u32][payload…]` into the guest's
output buffer and return `0` when it fit, else the required capacity.
Guest success returns the byte length of the encoded return `Value`; guest
rejection returns `0xFFFF_FFFF` with a length-prefixed message; ABI errors are
recorded **sticky** in host state so an ignored error can never commit.

### D5. Deterministic resource limits

Fuel (`Config::consume_fuel` + `Store::set_fuel`) is the primary execution
budget — deterministic, not wall-clock. `ResourceLimiter` caps linear memory.
A per-invocation host-call budget bounds host crossings. Byte budgets bound
args, return values, events, and scan results. wasmi `EnforcedLimits` bound
module parse/compile size. Defaults: 4 MiB memory, 1M fuel, 10k host calls,
8 KiB args/result, 1 KiB events, 64 KiB scans.

### D6. One transaction, one commit path

The WASM host translates ABI ops into the same `ReducerContext` methods the
native path uses, and both paths finish through the shared
`nexum-reducer::finish_invocation(store, tx, events, outcome)` helper — commit
on `Ok`, abort on `Err`, `Error::Conflict` propagated unchanged. The guest can
never begin/commit/abort transactions. WAL attach remains at the Phase 5
`commit() -> Vec<Change>` boundary; the result is a Phase 6 `ReducerResult`.

### D7. Failure = zero mutation

Traps, fuel exhaustion, memory-limit traps, malformed ABI input, sticky ABI
errors, malformed returns, guest rejection, and OCC conflicts all abort the
invocation. Writes are provisional (Phase 4), so no rollback machinery exists.
Events die with the invocation on every failure path.

### D8. Trust boundary is the ABI, not the runtime

wasmi provides the WebAssembly sandbox (memory isolation, control-flow
integrity); Nexum's ABI provides the capability boundary. Native reducers
remain trusted (Phase 6); WASM reducers are untrusted and may only reach the
transaction through the documented ops.

## Implementation notes (post-design)

- `WasmReducerModule` caches the **compiled** `wasmi::Module` plus the
  validated buffer addresses, not the raw bytecode — the design sketch's
  `bytecode`/`entry` fields became `compiled`/`in_ptr`/`out_ptr`, and the
  entry name is the fixed ABI constant `_nexum_reducer_run`.
- `WasmModuleRegistry::register(name, version, bytecode)` validates through
  `WasmReducerModule::new`, including a throwaway-store instantiation that
  reads the immutable buffer globals and checks the `ABI_IN_CAP`/`ABI_OUT_CAP`
  regions fit the declared memory.
- Start functions are not blanket-rejected: they run at `InstancePre::start`
  under the armed fuel/memory budgets, and a start function that attempts any
  state op is rejected at registration (the validation store has no
  transaction context, so the op fails with a sticky error). The host itself
  calls exactly one exported function.
- Fuel is armed **before** instantiation so any start function is metered
  deterministically.
- Guest-controlled counts (rows, key lists) are decoded with
  `Vec::try_reserve`, so a crafted `u64::MAX` count is a clean error — never a
  capacity-overflow panic or OOM abort (brief §9: malformed input returns an
  error, not a panic). Malformed op arguments surface as `InvalidArgument` at
  the ABI boundary; the host-call budget counts every host-function crossing,
  including the oversized-input rejection path.

## Consequences

**Positive.** Untrusted reducers with the same transactional semantics as
trusted ones; deterministic execution and budget enforcement; a minimal,
auditable ABI; WAL and future subscriptions (Phase 8) consume the identical
`Vec<Change>` boundary; no duplication anywhere in the state path.

**Negative / costs.** Interpreter throughput is lower than a JIT — accepted for
this phase (correctness/sandboxing first; benchmarks document the cost).
Hand-written guest code must encode/decode the ABI formats (a guest SDK is
future work). Rejection mapping is `InvalidArgument` (a richer error ABI can
evolve without breaking the wire format).

**Risks.** wasmi API churn between 0.x releases (pinned at 0.38; migration
notes in the crate docs). Fuel exhaustion surfaces as a wasmi error — the host
classifies it as a capacity error. The `Store::limiter` closure and host state
must remain borrow-safe (verified by construction: host ops copy guest memory
in, mutate state, copy results back).

## Alternatives considered

- **wasmtime / wasmer**: heavier, JIT-compile startup, larger build; wasmi's
  interpreter determinism and embedding simplicity win for this phase.
- **WASI-enabled**: rejected — capabilities we don't need are attack surface.
- **Host buffers instead of guest buffers**: rejected — copying results into
  guest memory with bounds checks keeps allocation on the guest's side and
  makes the ABI's memory ownership explicit.
- **Per-op named imports**: rejected in favor of a single `nexum.op` import —
  smaller validation surface, fewer crossing conventions.
