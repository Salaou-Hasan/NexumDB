# Phase 16 — Production Hardening, Scale Validation & Release: Design

Status: **implemented** (see `docs/reports/16-production.md` for measured results).

Phase 16 is the final planned phase. It turns the existing, working Nexum
stack (Phases 1–15) into a production-grade backend and **proves its
scalability honestly** — measuring the real CCU ceiling on the reference
hardware instead of claiming one.

## 0. Non-negotiable invariants (carried from Phases 1–15)

The authoritative model does not change:

```
Client → SDK → Network → GameServer → Runtime → Partition → World →
Simulation → Reducers/Systems → Transaction/OCC → ONE atomic commit →
Vec<Change> → WAL + SubscriptionRegistry → Network → SDK → Client
```

- ONE authoritative state owner (World/TableStore).
- ONE tick → ONE logical transaction → ONE atomic commit → ONE `Vec<Change>`.
- `World::tick` remains the only simulation commit path.
- WAL is durability; SubscriptionRegistry is observation; networking routes.
- `unsafe_code = forbid` everywhere.
- No second storage engine, transaction engine, or simulation runtime.

Phase 16 is **additive hardening**: configuration, rate limits, shutdown,
observability, deployment, and measurement — all around the existing core.

## 1. Scope

Phase 16 delivers:

1. Production configuration system (aggregated, validated, fail-fast).
2. Resource limits (every externally controllable resource bounded).
3. Authentication/authorization hardening (deny-by-default preserved).
4. Secure-transport architecture (adapter; TLS-ready, not fake-complete).
5. Rate limiting (bounded, configurable, observable).
6. Backpressure audit (slow clients never stall simulation).
7. Observability (structured logs + aggregate metrics).
8. Crash/recovery hardening (WAL truncation, corrupted tail, crash tests).
9. Failure isolation (reducer/WASM/world/partition/client).
10. CCU load validation (1K → 20K, honest ceiling, PASS/DEGRADED/SATURATED).
11. Release build profile + deployment documentation.

## 2. Production configuration

### 2.1 Design

A single validated `ServerConfig` (in the playable `game-server` crate)
aggregates every knob that can be set from the command line **or a config
file**, covering:

- network: bind host, port, max connections, frame payload bound, inbound/
  outbound queue bounds, subscriptions per session, pending calls per
  connection;
- game server: default partition count, max players per game, subscription
  limit per player, pending-command buffer;
- runtime: worker count, input queue bound, partition-message queue bound,
  reducer-call queue bound, persistence policy (None/Flush/Sync), snapshot
  interval, tick-failure policy;
- rate limits: auth attempts, input frames/sec, reducer calls/sec,
  subscription creates/sec, resyncs/sec (per connection/session);
- operational: tick rate (Hz), logging level, metrics interval, event-log
  limits;
- auth: token→principal table (static, deterministic; a real provider plugs
  into the existing `Authenticator` trait later);
- durability: WAL directory, snapshot directory, snapshot interval.

### 2.2 Validation

`ServerConfig::validate()` checks every bound (≥ 1, coherent ranges) and
returns a human-readable error. **Invalid configurations fail at startup**
before any world, connection, or WAL is created. Defaults are conservative
and documented; nothing dangerous is enabled silently (e.g. `Sync` WAL
durability is opt-in, never the default).

### 2.3 Loading order

1. Built-in defaults.
2. Config file (`--config FILE`, simple `key = value` lines, `#` comments).
3. Explicit CLI flags (override the file).

Higher-precedence wins. The effective configuration is logged at startup.

## 3. Rate limiting

### 3.1 Design

A per-connection (and per-session where relevant) **token bucket** in the
gateway, with one bucket per operation class:

| Operation | Bucket | Default |
|---|---|---|
| connection registration | global connection rate | configurable |
| authenticate | per connection, per window | configurable |
| input frame | per connection, per second | configurable |
| reducer call | per connection, per second | configurable |
| subscribe | per session, per window | configurable |
| resync | per connection, per window | configurable |

Buckets are bounded (capacity = burst, refill = steady rate). When a bucket
is empty the operation is **rejected with an explicit protocol error** (never
silently dropped, never panics), the rejection is counted in metrics, and a
structured log line is emitted.

### 3.2 Guarantees

- Rate limits are **operational** — they never alter simulation semantics.
- Accepted authoritative commands are never dropped by a rate limiter; the
  limiter only *rejects* before acceptance.
- Determinism is untouched: rate limiting happens outside `World::tick`.

## 4. Graceful shutdown

### 4.1 Design

A `ShutdownHandle` (an `Arc<AtomicBool>` shared with the server loop):

1. Signal (SIGINT/SIGTERM via `ctrlc`, or a stop-file, or `--stop-after N`
   ticks for scripted shutdown) flips the flag.
2. The server loop stops accepting new connections and drains inbound.
3. `GameServer::shutdown()` runs: runtime stops scheduling, **every world's
   WAL is flushed** (durability contract), resources released.
4. The process exits 0 after a final metrics/log summary.

Shutdown is idempotent and deterministic (ADR-016 D3).

## 5. Observability

### 5.1 Structured logging

A tiny leveled logger (`Error | Warn | Info | Debug`), configurable via
`log_level`, emitting `timestamp level module message` lines to stderr.
Structured key=value fields for hot events (connection, auth, join, failure,
backpressure, rate-limit). No heavyweight framework — justified by the
project's zero-dependency style.

### 5.2 Aggregate metrics

`ServerMetricsSnapshot` merges:

- `RuntimeMetrics` (ticks, inputs, WAL appends, worlds/partitions/workers);
- `NetworkMetrics` (connections, sessions, frames, drops, protocol errors,
  auth failures, rate-limit rejections);
- `GameServerMetrics` (games, players, joins/leaves, reducer calls);
- memory estimate (rows × bytes/row from Phase 15 + connections × per-client
  estimate) and process RSS where the platform exposes it.

Snapshots are point-in-time and never influence simulation.

## 6. Crash / recovery hardening

Existing Phase 5 WAL already: frames records, checksums every record
(CRC-32), length-bounds, truncates physically-invalid tails, and reports
`truncated_tail`. Phase 16 adds verification tests for:

- crash immediately after commit (WAL has the record; recovery replays it);
- interrupted snapshot write (partial snapshot ignored, WAL replay used);
- corrupted/truncated WAL tail (dropped, never guessed);
- recovery with active players + subscriptions: **recovered history is never
  replayed as live subscription events** (clients reattach/resync).

## 7. CCU load validation

### 7.1 Harness

A `ccu` load example in the `game-server` crate. It boots the **real stack**
(GameServer → Runtime → World with the arena game) and drives N simulated
clients through the **real gateway + real protocol codec + real SDK client
objects** over in-process transport (documented honestly: socket layer is
in-process; protocol, gateway, runtime, world, subscriptions are real).
Profiles:

- **A — connection only**: connect + authenticate + attach + stay.
- **B — light input**: each client sends movement at a realistic rate.
- **C — realistic game**: move + receive updates + occasional fire
  (reducer calls + subscription deltas).
- **D — stress**: high input/reducer/subscription pressure until saturation.

### 7.2 Acceptance classification

Per CCU level, record CPU, RAM, tick p50/p95/p99, input latency, queue
depth, drops, rejections. Classify:

- **PASS** — all healthy thresholds hold (tick p99 within budget, no
  uncontrolled queue growth, no drops of accepted work).
- **DEGRADED** — usable but p99 approaches the budget or queues grow.
- **SATURATED** — the ceiling: adding clients increases latency/queues
  non-linearly.
- **FAILED** — correctness violations (dropped accepted commands, state
  corruption, cross-client leakage).

Report the **actual** ceiling; never claim 20K unless the harness measures
it under a defined workload and criteria.

## 8. Release profile & deployment

- `.cargo/config.toml`: `[profile.release]` with LTO (fat), single
  codegen-unit, panic=unwind (keeps `catch_unwind` failure isolation),
  documented trade-offs.
- Deployment docs: Windows + Linux + container, data/WAL/snapshot dirs,
  config file, logging, metrics, graceful shutdown, restart/recovery,
  resource limits. No Kubernetes required.

## 9. What Phase 16 deliberately does NOT do

- No distributed clustering / replication / failover infrastructure
  (documented as horizontal-scaling boundary work, not implemented).
- No full external authentication provider (interface exists; provider is a
  later deployment concern).
- No TLS stack (transport abstraction documented; TLS is a deployment-time
  adapter — never simulated as complete).
- No second state/transaction/simulation system.

## 10. Design review checklist

- [x] One authoritative state system preserved.
- [x] No bypass of `World::tick` / OCC / `Vec<Change>`.
- [x] Every external resource bounded.
- [x] Rate limits explicit, observable, non-mutating.
- [x] Shutdown flushes WAL deterministically.
- [x] CCU claims will be backed by measurements.
- [x] `unsafe_code = forbid` maintained.
