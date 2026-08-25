# Phase 26 — Simulation Battery: workload-independent validation

## Goal

Replace "Nexum supports N CCU" (a single-workload claim) with a **Pareto
frontier**: supported CCU × simulation complexity × latency. A backend that
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

- **Density axis** (`--density`): entities ≠ connections. 20K players × 25
  units = 500K authoritative entities.
- **OS tuning** (`--os-tune`, default on): 1 ms timer resolution,
  HIGH_PRIORITY_CLASS, rayon-pool affinity.

## Measurement rules

1. Paced ticks (`--hz`); unpaced runs measure rate-limit rejection, not
   gameplay.
2. Server-only latency series (`server:` line) excludes the in-process
   client pump/drain — on real deployments that work runs on clients.
   Both series printed; never mixed.
3. Verdicts stay honest: PASS / DEGRADED (p99 ≤ 2× budget) / SATURATED,
   plus explicit zero-silent-loss and zero-failed-tick checks.
4. Every run emits one `SCORECARD,...` CSV line for matrix aggregation.

## Battery stages

- **v1 (this phase)**: archetypes, brains, density, scorecard, OS tune,
  pooling, absorb fix.
- **v2**: AOI sweep (window ∈ {10..1000} vs fan-out cost), cross-partition
  traffic injection at controlled rates through the deterministic message
  bus (Phase 12 machinery), WAL-enabled endurance run with mid-run kill →
  recovery → state comparison against the deterministic reference.
- **v3**: real network path (TCP transport now sets NODELAY; add latency/
  loss/jitter injection), thin third-party-engine clients (connect/auth/
  spawn/command/render/reconnect) to prove engine independence.

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
