# Phase 16 — Production Hardening, Scale Validation & Release: Architecture

Status: **implemented**. See `docs/reports/16-production.md` for measured
CCU/scale results and `docs/design/16-production.md` for the design.

## 0. Architectural position

Phase 16 adds an **operational shell** around the unchanged authoritative
core:

```
                    ┌──────────────────────────────────────────┐
                    │            Production Server            │
                    │  (game-server binary: config, logging,   │
                    │   metrics, shutdown, rate limits)        │
                    └──────────────────────────────────────────┘
                                      │
        Client → SDK → NetworkGateway → GameServer → Runtime → Partition → World
                                      │
                    World::tick → Transaction/OCC → ONE commit → Vec<Change>
                                                                   ├── WAL
                                                                   └── SubscriptionRegistry
                                                                        → Network → SDK → Client
```

Every Phase 16 feature is an **adapter or boundary** around this core:

| Concern | Owner | Never becomes |
|---|---|---|
| authoritative state | World/TableStore | network/game-server |
| transactions | Transaction/OCC | network/game-server |
| commit boundary | `Vec<Change>` | anything else |
| durability | WAL | network/game-server |
| observation | SubscriptionRegistry | network/game-server |
| identity | `Authenticator` trait + `Principal` | transport |
| rate limits | gateway (operational only) | simulation semantics |
| shutdown | server loop + `GameServer::shutdown` | state mutation |

## 1. Components

### 1.1 `ServerConfig` (game-server crate)

Aggregates and validates every operational knob:

```
ServerConfig
├── network: NetworkConfig (host, port, max_connections, frame/queue bounds,
│             subscriptions/session, pending calls/connection)
├── game_server: GameServerConfig (partitions, max_players, subscription
│                 limit/player, pending-command buffer)
├── runtime: RuntimeConfig (workers, queue bounds, persistence policy,
│             snapshot interval, tick-failure policy)
├── rate_limits: RateLimitConfig (auth, input/s, reducer/s, subscribe, resync)
├── logging: log_level, metrics_interval, event_log_limit
├── auth: static token→principal roster
└── durability: wal_dir, snapshot_dir
```

Loading order: defaults → `--config FILE` → explicit CLI flags.
`ServerConfig::validate()` fails startup on any invalid bound.

### 1.2 Rate limiting (nexum-network gateway)

Token buckets keyed by connection/session, one per operation class, applied
**before** any runtime call:

- `authenticate` — per connection (window).
- `input frame` — per connection (per second).
- `reducer call` — per connection (per second).
- `subscribe` / `resync` — per session (per window).

Behavior on empty bucket: explicit protocol error (code + message), metric
`rate_limited`, structured log line. No silent drop, no panic, no state
mutation. `RateLimitConfig` lives on `NetworkConfig` and is validated with
it. Defaults are generous (dev-friendly); production configs tighten them.

### 1.3 Graceful shutdown

`ShutdownHandle` = `Arc<AtomicBool>`. Triggered by:

1. `ctrlc` signal handler (SIGINT/SIGTERM → flag);
2. a stop-file path (operator drops a file);
3. `--stop-after N` ticks (scripted/deterministic shutdown for tests).

Server loop: stop accepting → drain inbound → `GameServer::shutdown()`
(runtime stops, **all WALs flushed**) → final metrics line → exit 0.
Idempotent; safe to call from the main thread after the loop observes the
flag.

### 1.4 Observability

- `Logger`: leveled (`Error|Warn|Info|Debug`), timestamped,
  `timestamp level module message` on stderr. Level from config.
- `ServerMetricsSnapshot`: merges `RuntimeMetrics` + `NetworkMetrics` +
  `GameServerMetrics` + memory estimate. Printed at `metrics_interval`
  (and on shutdown).

## 2. Security model (unchanged + hardened)

- Deny-by-default reducer exposure (Phase 14) preserved; rate limits add a
  second, operational layer **in front of** the policy.
- Anti-spoofing: the gateway stamps `__caller` from the authenticated
  principal; client-supplied identity fields are ignored (Phase 13/14).
- All client-controlled sizes bounded by `NetworkConfig` (frame payload,
  commands/frame, args, pending calls, subscriptions) — Phase 11 D3–D5.
- Rate-limit rejection is explicit and observable; connection flooding is
  bounded by `max_connections` + connection rate.
- `unsafe_code = forbid`.

## 3. ADR-016 — decisions

### D1. Rate limiting is a gateway token bucket, not a simulation feature

Decision: per-connection token buckets in `nexum-network`, applied before
runtime dispatch.

Rationale: rate limits are operational policy. Putting them in the gateway
keeps simulation semantics untouched (determinism) and applies uniformly to
every transport. Consequences: rate limiting is never part of tick logic;
accepted commands are never rate-limited after acceptance.

### D2. Signal handling uses `ctrlc`

Decision: add the `ctrlc` crate to the `game-server` binary for SIGINT/
SIGTERM → shutdown-flag. Stop-file and `--stop-after N` remain as
dependency-free, scriptable alternatives.

Rationale: `ctrlc` is the smallest standard crate for cross-platform
console-signal handling (Windows + POSIX), and the project otherwise has
zero runtime deps outside the workspace. The flag it sets only triggers the
existing deterministic shutdown path.

### D3. Shutdown is drain-then-flush, idempotent

Decision: on shutdown the loop stops accepting connections, drains inbound,
calls `GameServer::shutdown()` (which calls `Runtime::shutdown()` — already
flush-safe and idempotent per Phase 10), then exits.

Rationale: WAL flush on shutdown is the durability contract (Phase 5 D7);
idempotence makes double-signal safe.

### D4. CCU results are measured, classified, and reported honestly

Decision: the `ccu` harness measures the real stack (real gateway, protocol,
runtime, world, SDK objects) over in-process transport; results are
classified PASS/DEGRADED/SATURATED/FAILED against explicit thresholds; the
report states the measured ceiling and the hardware it was measured on.

Rationale: the phase's honesty rule — never claim 20K CCU without a
reproducible measurement under a defined workload.

### D5. Release profile: LTO + single codegen unit, panic=unwind

Decision: `[profile.release]` with `lto = "fat"`, `codegen-units = 1`,
`panic = "unwind"` (keeps `catch_unwind`-based failure isolation),
`strip = "debuginfo"` optional.

Rationale: measured Phase 15 micro-benchmarks dominate the release config;
LTO/single-CGU give the largest codegen win with no correctness cost.
panic=unwind is required so a panicking reducer/WASM host never takes down
the process (failure isolation, ADR-010 D6).

## 4. CCU harness architecture

```
ccu example
├── ServerConfig (from defaults/file/CLI)
├── real stack: GameServer → Runtime → World (arena game factory)
├── N simulated clients
│   ├── real SDK Client objects
│   ├── real protocol codec (encode/decode)
│   └── in-process transport (documented: socket layer simulated)
├── profiles A (connect) / B (light input) / C (realistic) / D (stress)
└── measurement loop: tick p50/p95/p99, input latency, queue depth,
    drops, rejections, CPU, RAM → classification
```

The harness exercises the full authoritative pipeline
`input → gateway → runtime → World::tick → commit → Vec<Change> → WAL →
SubscriptionRegistry → network → SDK view` for every simulated client.

## 5. Scale targets and honesty rules

- Attempt 1K / 2.5K / 5K / 10K / 15K / 20K CCU; continue past 20K only to
  find the true ceiling.
- Record CPU, RAM, tick p50/p95/p99, input latency, queue depth, drops,
  rejections at every level.
- Classify PASS/DEGRADED/SATURATED/FAILED with reasons.
- Never extrapolate; never claim unmeasured capacity.
- If the machine cannot complete a level safely, document it (like Phase 15
  documented the 10M-row ceiling).
