# Phase 26 — Simulation Battery: workload-independent validation

## Goal

Replace "Nexum supports N CCU" (a single-workload claim) with a **Pareto
frontier**: supported CCU x simulation complexity x latency. A backend that
survives 20K idle connections and fails at 2K intensive simulation is not a
general-purpose simulation engine; the battery makes that boundary visible
and continuous.

## Structure

Two independent axes plus modifiers:

- **CCU ladder**: 200 → 1K → 5K → 10K → 15K → 20K clients across `--lobbies`
  (independent worlds; the intended deployment shape).
- **Workload archetypes** (`--workload`), driven by deterministic seeded
  player brains (`--seed`, splitmix64 — identical command streams across runs
  and worker counts):

| archetype | pattern exercised | mix highlights |
|---|---|---|
| SOCIAL    | idle connections, cheap RPC, sparse writes       | 5% move, 2% presence |
| FPS       | latency-critical movement + WASM combat          | 70% move, 6% fire    |
| MMO       | mixed combat/economy/social                      | balanced spread      |
| RTS       | simulation density via `--density` units/player  | 25 unit_moves/tick   |
| SURVIVAL  | read-modify-write economy, persistence writes     | gather (RMW+insert)  |
| EXTREME   | legacy profile E (everyone moves every tick)      | 100% move + bursts   |

New gameplay surface (all native, O(log N) discipline): `units` table +
`unit_move` (ownership-checked entity movement), `inventory` table + `gather`
(score RMW + insert + event), `presence` (cheap full-path RPC).

- **Density axis** (`--density`): entities ≠ connections. 20K players x 25
  units = 500K authoritative entities.
- **OS tuning** (`--os-tune`, default on): 1 ms timer resolution,
  HIGH_PRIORITY_CLASS, rayon-pool affinity.

## Measurement rules

1. Paced ticks (`--hz`); unpaced runs measure rate-limit rejection, not
   gameplay.
2. Server-only latency series (`server:` line) excludes the in-process
   client pump/drain — on real deployments that work runs on clients.
   Both series printed; never mixed.
3. Verdicts stay honest: PASS / DEGRADED (p99 ≤ 2x budget) / SATURATED,
   plus explicit zero-silent-loss and zero-failed-tick checks.
4. Every run emits one `SCORECARD,...` CSV line for matrix aggregation.

## Battery stages

- **v1**: archetypes, brains, density, scorecard, OS tune, pooling, absorb fix.
- **v2 (this phase)**:
  - **Cross-partition injection** (`--xpart P`): P‰ of clients per tick
    submit a `relay` host command; the `relay_station` system forwards each
    across the Phase 12 deterministic bus to the next partition, where the
    registered `relay_recv` handler reducer executes in Phase 0a. Requires
    `--partitions > 1`. Harness fix required: the timed path now goes
    through `GameServer::step_authoritative` so buffered host commands are
    actually flushed into the tick (the previous direct-runtime call
    silently skipped them).
  - **AOI sweep**: `--window W` is the interest-management knob; swept on
    MMO@5K (20x250p):
    | window | server p50 | server p95 | sub_deltas |
    |---|---|---|---|
    | 10   | 9.7 ms  | 31.9 ms | 559 K |
    | 25   | 10.2 ms | 32.5 ms | 598 K |
    | 50   | 10.6 ms | 33.2 ms | 600 K |
    | 100  | 11.7 ms | 35.9 ms | 600 K |
    | 250  | 14.0 ms | 40.3 ms | 600 K |
    | 1000 | 14.1 ms | 42.5 ms | 600 K |
    Tighter AOI genuinely buys latency: −45% p50 from window 1000→10;
    deltas cap once the window covers all reachable changes.
  - **Persistence plumbing** (`--persist DIR`): per-world WALs under load
    verified (Flush policy). Automated mid-run kill → `recover_world` →
    state-hash comparison is v3.
- **v3**: kill/recovery automation; network impairment (latency/loss/jitter)
  on the TCP path; thin third-party-engine clients.

## v2 results — cross-partition ladder (MMO@5K, 5 lobbies x 4 partitions)

| rate | sent | delivered | dropped | server p99 | verdict |
|---|---|---|---|---|---|
| 0‰   | 0 | 0 | 0 | 37.3 ms | PASS |
| 10‰  | 5,916 | 5,862 | 0 | 36.7 ms | PASS |
| 30‰  | 17,929 | 17,772 | 0 | 38.5 ms | PASS |
| 50‰  | 30,000 | 29,745 | 0 | 51.3 ms | DEGRADED |

The bus carries ~250 msgs/tick at 50% injection with zero drops; the tail
cost of cross-partition fan-in is real but modest at these rates.

## v1 results (i7-14650HX, release, paced 20 Hz, server-only)

See `SCORECARD` lines in the phase report; headline cells:

| workload | 1K CCU | 20K CCU |
|---|---|---|
| SOCIAL   | 0.79 ms PASS  | 8.9 ms DEGRADED |
| FPS      | 2.57 ms PASS  | 42.7 ms SATURATED |
| MMO      | 2.41 ms PASS  | 54.4 ms SATURATED |
| SURVIVAL | 1.84 ms PASS  | 42.9 ms SATURATED |
| RTS d=25 | 0.23 ms PASS* | 245.9 ms SATURATED |
| EXTREME  | 2.78 ms PASS  | 37.9 ms SATURATED |

\* RTS p99 spikes (unit-insert storm in early ticks) need the AOI/batching
work in v2; average world time is within budget at 1K.

Reading: 20K CCU holds only near-idle workloads on one node; realistic
gameplay saturates between 5K–10K CCU on this hardware class. The frontier —
not a single number — is the product claim.
