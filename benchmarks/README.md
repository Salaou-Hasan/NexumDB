# Benchmarks

Benchmarks land in Phase 15. Per the spec, each subsystem is benchmarked
independently first (table insert/lookup, index lookup, transaction commit and
conflict, WAL append, snapshot, reducer execution, subscription matching, delta
generation, simulation tick), then realistic mixed workloads (10,000 players,
multiple zones, mixed reducers, subscriptions, simulation, network traffic,
persistence).

No optimization before measurement.
