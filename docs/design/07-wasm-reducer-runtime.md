# Phase 7 Design — WASM Reducer Runtime

Status: **design** (implementation follows in this phase)
Dependencies: Phases 1–6 (tables, storage, OCC transactions, WAL, native reducers).
Out of scope: subscriptions, simulation, networking, distribution, WASI.

## 1. Purpose

Phase 7 is the **execution/sandbox layer** for untrusted reducer programs. A
WASM reducer is a user-provided module that runs inside a sandbox and changes
authoritative state **only through a restricted host ABI** that translates into
the existing Phase 6 `ReducerContext` → `Transaction` → OCC → commit path.

```
WASM reducer
    ↓
restricted host ABI ("nexum.op", one import)
    ↓
ReducerContext
    ↓
Transaction
    ↓
OCC validation
    ↓
atomic commit
    ↓
Vec<Change>  →  WAL (unchanged Phase 5 boundary)
```

ONE STATE. ONE TRANSACTION MODEL. ONE OCC IMPLEMENTATION. ONE COMMIT PATH.
Phase 7 introduces **no** second storage, transaction, or OCC implementation.

## 2. Runtime choice: wasmi 0.38 (evaluated)

| Criterion | wasmi 0.38 | wasmtime | wasmer |
|---|---|---|---|
| Sandboxing | WebAssembly spec sandbox | spec sandbox | spec sandbox |
| Determinism | pure interpreter, spec-deterministic | JIT, spec-deterministic | JIT/interpreter |
| Instruction budget | **first-class fuel** (`Config::consume_fuel`, `Store::set_fuel`) | fuel possible, more wiring | fuel via engine config |
| Memory ceiling | `ResourceLimiter`/`StoreLimits` (host-controlled, module-agnostic) | `ResourceLimiter` | engine config |
| Startup cost | low (no JIT) | JIT compile | depends |
| Embedding | pure Rust, `Store<T>` host data with **no `'static` bound**, `Linker` | mature but heavyweight | larger surface |
| Build weight | tiny, fast | cranelift (minutes) | moderate |
| WASI | **absent unless added** | optional | optional |
| Maintainability | maintained by the Bytecode Alliance (wasmi-labs / Wasmtime team) | Bytecode Alliance | independent |

Decision: **wasmi 0.38** (latest stable 0.x; 2.0 is a beta rewrite). Interpreter
execution is deterministic by construction; fuel and memory limits are
host-controlled; the dependency tree stays small and pure-Rust; there is no
WASI surface at all unless we add it (we don't). `wat` (dev-dependency) parses
WAT text in tests; no WASM toolchain is required to build or test.

## 3. Sandbox boundary

A WASM reducer is **untrusted code**. It never receives:
`&mut TableStore`, `&TableStore`, `Transaction`, `StorageTable`, `ReducerContext`
itself, Rust references, raw pointers into Nexum memory, file handles, sockets,
or any arbitrary host object. The **only** import a module may declare is the
single host function `("nexum", "op")`; module registration **rejects any
other import** (so `wasi_snapshot_preview1`, `env.*`, `spectest`, ... are
impossible by construction). No WASI is linked, ever.

All host↔guest data crosses the boundary as **bytes in the guest's own linear
memory**, read and written by the host with bounds checks (wasmi `Memory::read`/
`write`). No Rust object identity or pointer ever crosses the ABI.

## 4. ABI

### 4.1 Module contract (exports)

A valid Nexum reducer module exports:

| Export | Kind | Meaning |
|---|---|---|
| `memory` | memory | the module's linear memory |
| `_nexum_in_ptr` | global (immutable `i32`) | base address of the module's input buffer |
| `_nexum_out_ptr` | global (immutable `i32`) | base address of the module's output buffer |
| `_nexum_reducer_run` | `() -> i32` | the reducer entry point |

Buffer contract (validated at registration): the regions
`[in_ptr, in_ptr + ABI_IN_CAP)` and `[out_ptr, out_ptr + ABI_OUT_CAP)` must lie
inside the declared memory (`ABI_IN_CAP = 16 KiB`, `ABI_OUT_CAP = 64 KiB`). The
host never needs to know the guest's allocation strategy.

### 4.2 Host import

Exactly one import: `("nexum", "op")` with signature
`(i32, i32, i32, i32, i32) -> i32`:

```
op(opcode, in_ptr, in_len, out_ptr, out_cap) -> u32
```

- Input: the encoded operation arguments at `in_ptr..in_ptr+in_len`.
- Output: the host writes an **envelope** at `out_ptr`:
  `[status: u32][payload_len: u32][payload…]`, at most `out_cap` bytes.
- Return: `0` = envelope written; `> 0` = envelope did not fit, return value is
  the required capacity (the guest may retry with a larger buffer).

On any ABI error the status is nonzero and the payload is the error message;
the host also records the failure **sticky** in its per-invocation state, so an
ignored error can never lead to a commit.

### 4.3 Opcodes

| Op | Code | Input encoding | Result payload |
|---|---|---|---|
| `GET` | 1 | table name (str), row_id (u64) | `0x00` absent · `0x01` + row |
| `CONTAINS` | 2 | table name, row_id | `bool` byte |
| `SCAN` | 3 | table name | u64 count + (row_id, row)xn |
| `LOOKUP_UNIQUE` | 4 | table name, index name (str), key (`Vec<Value>`) | u64 count + row_idxn |
| `INSERT` | 5 | table name, row | u64 (provisional row id) |
| `UPDATE` | 6 | table name, row_id, row | — |
| `DELETE` | 7 | table name, row_id | — |
| `EMIT` | 8 | event name (str), payload (`Value`) | — |

Encodings reuse `nexum-core::binary` (`put_str`/`put_u64`/`put_row`/`put_value`
etc.): little-endian, deterministic, length-prefixed, bounds-checked; malformed
input is an ABI error, never a panic.

### 4.4 Entry point contract

`_nexum_reducer_run() -> i32`:

- **Success**: return `N` = byte length of the encoded return `Value` written by
  the guest at `out_ptr` (`N ≤ out_cap`, host validates `N ≤ max_result_bytes`).
- **Application rejection**: write `[msg_len: u32][utf8 message]` at `out_ptr`
  and return `0xFFFF_FFFF`.
- **ABI failure / trap / fuel / limits**: the host aborts regardless of the
  return value (sticky error, trap, or limit breach).

## 5. Resource limits (`WasmLimits`, defaults)

| Limit | Default | Enforced by |
|---|---|---|
| `max_memory_bytes` | 4 MiB (64 pages) | `ResourceLimiter::memory_growing` on every `memory.grow` |
| `max_fuel` | 1_000_000 | wasmi fuel metering (deterministic instruction budget) |
| `max_host_calls` | 10_000 | per-invocation counter in host state |
| `max_args_bytes` | 8 KiB | host before writing args into guest memory |
| `max_result_bytes` | 8 KiB | host when reading the return value |
| `max_event_bytes` | 1 KiB | host on `EMIT` payload |
| `max_scan_bytes` | 64 KiB | host on `SCAN` result encoding |
| module compile limits | wasmi `EnforcedLimits` | engine at `Module::new` (parse/compile DoS) |

Fuel, not wall-clock time, is the primary execution budget (deterministic);
the host call budget bounds host-function crossings. A malicious reducer cannot
consume unbounded CPU (fuel), memory (limiter + memory bounds), or host work
(call budget).

## 6. Determinism

- wasmi is a pure interpreter: WebAssembly execution is spec-deterministic.
- Fuel consumption is deterministic (same code + input ⇒ same fuel).
- No WASI: no filesystem, network, environment, clock, or randomness imports
  exist in the linker; unknown imports are rejected at registration.
- Host operations delegate to the deterministic transaction layer: scans return
  rows in ascending `RowId` order, `lookup_unique` results are sorted, args are
  encoded key-sorted (BTreeMap), events are ordered by `EMIT`, errors are
  fixed-format strings, and commit ordering is the Phase 4 deterministic order.
- The guest's `memory.grow` succeeds/fails deterministically.

Guarantee: the same module + same args + same transaction-visible state
produces the same committed result (and the same fuel profile).

## 7. Transaction semantics

The host translates every ABI op into the existing `ReducerContext` method of
the same name. WASM reducers therefore inherit, with **zero duplicated logic**:
read-your-writes, point-read OCC, write/write conflicts, missing-row conflicts,
delete conflicts, unique-key validation, table-epoch phantom protection,
multi-table atomicity, deterministic commit ordering, provisional insert
handles, and transaction-local events. There is exactly **one** authoritative
transaction per invocation, owned by the host (the guest can never commit,
abort, or begin one).

The WASM invocation reuses the Phase 6 result contract exactly: on success it
returns a `ReducerResult { tx_id, changes, events, return_value }`; the caller
appends `changes` to the WAL with `tx_id` — the Phase 5 boundary is untouched.

## 8. Failure semantics

| Failure | Behavior |
|---|---|
| WASM trap (`unreachable`, OOB, type error) | invoke fails; tx aborted; zero writes/events; no WAL record |
| Fuel exhausted | invoke fails with a capacity error; aborted; nothing committed |
| Memory limit exceeded (`memory.grow`) | trap; aborted |
| Malformed ABI input / invalid opcode | sticky ABI error; aborted |
| Unknown table / index / row | `NotFound` via the ABI; sticky; aborted |
| Invalid argument (bad types) | `InvalidArgument` via the ABI; aborted |
| OCC `Conflict` at commit | `Error::Conflict` propagates unchanged (retry by caller) |
| Application rejection (guest returns `0xFFFF_FFFF`) | `InvalidArgument` carrying the guest message; aborted |
| Guest return value malformed/oversized | aborted |

All paths keep authoritative state unchanged because writes were provisional
(Phase 4) — no rollback machinery exists or is needed. Events die with the
invocation on every failure path.

## 9. Module registry

```rust
pub struct WasmModuleRegistry {
    modules: BTreeMap<String, WasmReducerModule>,   // deterministic listing
    engine: Engine,                                 // fuel-enabled, shared
    limits: WasmLimits,
}
pub struct WasmReducerModule { name, version: u64, bytecode: Vec<u8>, entry: String }
```

- `register(module)` validates: WASM parses+validates (`Module::new`), the only
  import is `("nexum","op")` with the exact signature, the four exports exist
  with the right kinds, and the buffer regions fit the declared memory.
  Compiled modules are cached for reuse. Duplicate name → `AlreadyExists`.
- `lookup(name)`, `list()` (ascending name), `len`.
- `invoke(&self, store, name, args) -> Result<ReducerResult>`:
  begin one transaction → build `ReducerContext` → run the module with a fresh
  host state (fuel, limits, sticky error) → `finish_invocation` (the Phase 6
  shared commit/abort helper) → `ReducerResult`.

Versioning is a single `u64` counter in this phase (metadata for future
deployments); no distribution system.

## 10. Native + WASM parity

```
Native reducer          WASM reducer
    │                       │
    └── ReducerContext ◄────┘   (host translates ABI ops into the same calls)
            │
        Transaction
            │
         OCC commit
            │
        Vec<Change>
```

`finish_invocation(store, tx, events, outcome)` — a new public helper in
`nexum-reducer` — is the **single** commit/abort decision point shared by both
paths. No OCC, storage, or table logic is duplicated.

## 11. Testing plan (mapped to the brief)

- **Security**: registration rejects modules importing anything other than
  `("nexum","op")` (e.g. a `wasi_snapshot_preview1` import); no WASI linked;
  host never exposes store/transaction references (structural).
- **Limits**: fuel exhaustion (infinite loop), memory growth beyond the limit
  (`memory.grow`), oversized args/return/event payloads.
- **Correctness**: GET / CONTAINS / SCAN / LOOKUP_UNIQUE / INSERT / UPDATE /
  DELETE / EMIT through the ABI.
- **Transaction semantics**: read-your-writes through WASM
  (insert→get, insert→update→get, insert→delete→get), unique-key violation,
  phantom scan conflict, multi-table atomicity, conflict propagation — at the
  same boundary the native path is tested (a single-threaded invoke cannot race
  itself).
- **Failure**: trap → abort with zero mutation/events; ABI error → abort;
  malformed return → abort.
- **Integration**: WASM reducer → commit → `Vec<Change>` → WAL append → crash →
  `recover()` → identical state (Phase 5 mechanism unchanged).

## 12. Benchmarks

`examples/wasm_bench.rs`: empty, read-only, single-row read, single-row write,
10-row write, multi-table, event emission, scan, trap, fuel exhaustion.
Module compile/instantiate happens outside the timed loop (compiled modules are
cached by the registry); numbers are honest baselines — no claims of superiority
over native without measurement. Criterion harnesses land in Phase 15.

## 13. Non-goals

Subscriptions, simulation runtime, networking, distributed transactions,
clustering, replication, sharding, authentication, cloud deployment, SDKs,
WASI, hot module reload, module version resolution beyond a `u64` tag.
