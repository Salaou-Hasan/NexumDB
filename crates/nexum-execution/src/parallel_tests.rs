//! Phase 11 unit tests (ADR-011): deterministic parallel tick execution.
//!
//! The central claim under test: `ExecutionMode::Serial` and
//! `ExecutionMode::Parallel(N)` produce **identical** per-tick change/event
//! traces and final authoritative state for any N, on identical worlds —
//! proving worker-count independence and that the Phase 9 serial path
//! remains the oracle.

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, Error, ReducerId, Row, RowId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use crate::input::InputFrame;
use crate::parallel::TickPlan;
use crate::systems::{SystemAccess, SystemDefinition};
use crate::{ExecutionMode, Partition, PartitionConfig};

/// A world with three single-column tables `a`, `b`, `c` (ids 0, 1, 2).
fn fixture(config: PartitionConfig) -> Partition {
    let mut store = TableStore::new();
    for name in ["a", "b", "c"] {
        store
            .create_table(
                TableSchema::builder(name)
                    .column("id", ColumnType::U64)
                    .primary_key(&["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
    Partition::new(WorldId::from_u64(0), store, config).unwrap()
}

fn frame(tick: u64) -> InputFrame {
    InputFrame::new(TickId::from_u64(tick))
}

/// One tick's committed (changes, events) pair.
type TickOutcome = (Vec<nexum_storage::Change>, Vec<nexum_reducer::ReducerEvent>);
/// The per-tick trace plus the final store dump.
type Trace = (Vec<TickOutcome>, Vec<(String, Vec<(RowId, Row)>)>);

fn dump(store: &TableStore) -> Vec<(String, Vec<(RowId, Row)>)> {
    let mut out = Vec::new();
    for (name, table) in store.tables() {
        let rows: Vec<(RowId, Row)> = table.scan().map(|(id, r)| (id, r.clone())).collect();
        out.push((name.to_string(), rows));
    }
    out
}

/// Runs `ticks` ticks of `world`, returning per-tick outcomes and the dump.
fn trace_of(world: &mut Partition, ticks: u64) -> Trace {
    let mut trace = Vec::new();
    for tick in 0..ticks {
        let result = world.tick(&frame(tick)).expect("tick committed");
        trace.push((result.changes().to_vec(), result.events().to_vec()));
    }
    (trace, dump(world.store()))
}

// -------------------------------------------------------------- tick plan

#[test]
fn plan_groups_disjoint_systems_and_splits_conflicts() {
    let mut world = fixture(PartitionConfig::new());
    let sys =
        |id: u64, name: &str, priority: u32, access: SystemAccess| {
            SystemDefinition::with_access(SystemId::from_u64(id), name, priority, access, |_, _| {
                Ok(())
            })
            .unwrap()
        };
    world
        .add_system(sys(0, "a_writer", 10, SystemAccess::new(&[], &["a"])))
        .unwrap();
    world
        .add_system(sys(1, "bc_writer", 20, SystemAccess::new(&[], &["b", "c"])))
        .unwrap();
    // Conflicts with bc_writer (writes b) and a_writer (reads a): own group.
    world
        .add_system(sys(2, "rng_user", 30, SystemAccess::new(&["a"], &["b"])))
        .unwrap();
    // Opaque: always its own singleton group.
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(3), "opaque", 40, |_, _| Ok(())).unwrap(),
        )
        .unwrap();

    let plan = TickPlan::build(world.systems(), world.store()).unwrap();
    let groups: Vec<Vec<usize>> = plan.groups().iter().map(|g| g.systems().to_vec()).collect();
    // [a_writer, bc_writer] are table-disjoint → one group; then rng_user;
    // then the opaque singleton.
    assert_eq!(groups, vec![vec![0, 1], vec![2], vec![3]]);
}

#[test]
fn plan_is_deterministic_and_rejects_unknown_tables() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "ghost",
                0,
                SystemAccess::new(&["nope"], &[]),
                |_, _| Ok(()),
            )
            .unwrap(),
        )
        .unwrap();
    let plan = TickPlan::build(world.systems(), world.store());
    assert!(matches!(plan, Err(Error::InvalidArgument(_))));

    // Determinism: the same systems/table ids always produce the same plan.
    let mut world = fixture(PartitionConfig::new());
    let sys = |id: u64, name: &str, access: SystemAccess| {
        SystemDefinition::with_access(SystemId::from_u64(id), name, 0, access, |_, _| Ok(()))
            .unwrap()
    };
    world
        .add_system(sys(0, "x", SystemAccess::new(&["a"], &["b"])))
        .unwrap();
    world
        .add_system(sys(1, "y", SystemAccess::new(&["b"], &["c"])))
        .unwrap();
    world
        .add_system(sys(2, "z", SystemAccess::new(&[], &["a"])))
        .unwrap();
    let a = TickPlan::build(world.systems(), world.store()).unwrap();
    let b = TickPlan::build(world.systems(), world.store()).unwrap();
    assert_eq!(a, b);
}

// ------------------------------------------- worker-count independence

/// A rich world exercising disjoint systems, a same-table write chain, RNG,
/// a native reducer, and a scheduled event — all inside one tick.
fn rich_world(config: PartitionConfig) -> Partition {
    let mut world = fixture(config);
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "a_writer",
                10,
                SystemAccess::new(&[], &["a"]),
                |ctx, _| {
                    ctx.insert("a", row![ctx.tick().as_u64()])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(1),
                "bc_writer",
                20,
                SystemAccess::new(&[], &["b", "c"]),
                |ctx, _| {
                    let tick = ctx.tick().as_u64();
                    ctx.insert("b", row![tick])?;
                    ctx.insert("c", row![tick + 100])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    // Writes b (same table as bc_writer) and reads a: never grouped with
    // either — exercises cross-group provisional-id continuity.
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(2),
                "rng_user",
                30,
                SystemAccess::new(&["a"], &["b"]),
                |ctx, _| {
                    let value = ctx.rng().next_below(1000);
                    ctx.insert("b", row![1000 + value])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    world
        .native_mut()
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "mark", |ctx, args| {
                let mark = args.require_u64("m")?;
                ctx.insert("c", row![mark])?;
                ctx.emit("marked", mark)?;
                Ok(Value::U64(mark))
            })
            .unwrap(),
        )
        .unwrap();
    // Opaque: the reducer touches table c, so it cannot be parallelized.
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(3), "invoker", 40, |ctx, _| {
                let mark = 5000 + ctx.tick().as_u64();
                ctx.invoke_reducer("mark", &ReducerArgs::new().insert("m", mark))?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .schedule(
            TickId::from_u64(2),
            "mark",
            ReducerArgs::new().insert("m", 999u64),
        )
        .unwrap();
    world
}

#[test]
fn worker_count_never_changes_the_trace() {
    let ticks = 8;
    let serial = trace_of(
        &mut rich_world(PartitionConfig::new().with_execution(ExecutionMode::Serial)),
        ticks,
    );
    for workers in [1usize, 2, 4, 8] {
        let parallel = trace_of(
            &mut rich_world(
                PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)),
            ),
            ticks,
        );
        assert_eq!(serial, parallel, "Parallel({workers}) diverged from serial");
    }
}

#[test]
fn repeated_parallel_runs_with_same_seed_are_identical() {
    let a = trace_of(
        &mut rich_world(
            PartitionConfig::new()
                .with_seed(7)
                .with_execution(ExecutionMode::Parallel(4)),
        ),
        10,
    );
    let b = trace_of(
        &mut rich_world(
            PartitionConfig::new()
                .with_seed(7)
                .with_execution(ExecutionMode::Parallel(4)),
        ),
        10,
    );
    assert_eq!(a, b);
}

#[test]
fn different_seeds_diverge_in_parallel_mode() {
    let a = trace_of(
        &mut rich_world(
            PartitionConfig::new()
                .with_seed(1)
                .with_execution(ExecutionMode::Parallel(4)),
        ),
        6,
    );
    let b = trace_of(
        &mut rich_world(
            PartitionConfig::new()
                .with_seed(2)
                .with_execution(ExecutionMode::Parallel(4)),
        ),
        6,
    );
    assert_ne!(a, b);
}

// --------------------------------------------------- cross-group visibility

#[test]
fn later_group_sees_earlier_groups_provisional_writes() {
    let run = |config: PartitionConfig| {
        let mut world = fixture(config);
        // Seed row 0 of table a.
        let mut tx = Transaction::begin(world.store_mut());
        tx.insert(world.store(), "a", row![1u64]).unwrap();
        tx.commit(world.store_mut()).unwrap();

        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "writer",
                    10,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.update("a", RowId::from_u64(0), row![9u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        // Reads a → conflicts with writer → separate group; must observe the
        // writer's *provisional* update through the branch inheritance.
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(1),
                    "reader",
                    20,
                    SystemAccess::new(&["a"], &[]),
                    |ctx, _| {
                        let got = ctx
                            .get("a", RowId::from_u64(0))?
                            .expect("reader must see the writer's provisional update");
                        if got != row![9u64] {
                            return Err(Error::internal(format!(
                                "reader saw {got:?}, expected the provisional row"
                            )));
                        }
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let (trace, dump) = trace_of(&mut world, 1);
        assert_eq!(trace[0].0.len(), 1); // one committed update
        assert_eq!(trace[0].0[0].kind(), nexum_core::ChangeKind::Update);
        (dump, world)
    };

    let (serial_dump, _) = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    let (parallel_dump, _) = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(4)));
    assert_eq!(serial_dump, parallel_dump);
}

#[test]
fn same_table_interleaved_writes_assign_identical_real_ids() {
    // Two systems write table a (different rows). Serial assigns provisional
    // handles in system order; parallel must produce the same real ids.
    let run = |config: PartitionConfig| -> Vec<(RowId, Row)> {
        let mut world = fixture(config);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "first",
                    10,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.insert("a", row![1u64])?;
                        ctx.insert("a", row![2u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(1),
                    "second",
                    20,
                    SystemAccess::new(&["a"], &["b"]),
                    |ctx, _| {
                        ctx.insert("a", row![3u64])?;
                        ctx.insert("b", row![30u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        // second writes a too → conflicts with first → separate group.
        trace_of(&mut world, 1);
        world
            .store()
            .table("a")
            .unwrap()
            .scan()
            .map(|(id, r)| (id, r.clone()))
            .collect()
    };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial, parallel, "Parallel({workers}) diverged");
    }
    // Sanity: rows are [1,2,3] in ascending RowId order (call order).
    let values: Vec<u64> = serial
        .iter()
        .map(|(_, r)| r.get(0).unwrap().as_u64().unwrap())
        .collect();
    assert_eq!(values, vec![1, 2, 3]);
}

// ------------------------------------------------------------ read-your-writes

#[test]
fn read_your_writes_holds_inside_parallel_children() {
    for workers in [1usize, 2, 4] {
        let mut world =
            fixture(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "ryw",
                    0,
                    SystemAccess::new(&["a"], &["a"]),
                    |ctx, _| {
                        let handle = ctx.insert("a", row![1u64])?;
                        let got = ctx.get("a", handle)?;
                        if got.as_ref() != Some(&row![1u64]) {
                            return Err(Error::internal("insert not visible to get"));
                        }
                        ctx.update("a", handle, row![7u64])?;
                        let got = ctx.get("a", handle)?;
                        if got.as_ref() != Some(&row![7u64]) {
                            return Err(Error::internal("update not visible to get"));
                        }
                        if ctx.scan("a")?.len() != 1 {
                            return Err(Error::internal("scan did not overlay writes"));
                        }
                        ctx.delete("a", handle)?;
                        if ctx.get("a", handle)?.is_some() {
                            return Err(Error::internal("delete not visible to get"));
                        }
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        // insert→delete is a net no-op: zero committed changes.
        let (changes, _) = trace_of(&mut world, 1);
        assert!(changes[0].0.is_empty(), "Parallel({workers})");
        assert!(world.store().table("a").unwrap().is_empty());
    }
}

// ------------------------------------------- undeclared dependencies

#[test]
fn lying_write_declaration_is_detected_not_silently_wrong() {
    // Both systems declare `SystemAccess::new(&[], &[])` (declared-empty, not
    // opaque — the planner puts them in one group) but both actually write
    // table `a`. In serial the tick commits both rows; in parallel the merge
    // must detect the undeclared write/write overlap deterministically
    // instead of silently overwriting one child's row.
    let mut serial = fixture(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    serial
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "liar_a",
                10,
                SystemAccess::new(&[], &[]),
                |ctx, _| {
                    ctx.insert("a", row![1u64])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    serial
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(1),
                "liar_b",
                20,
                SystemAccess::new(&[], &[]),
                |ctx, _| {
                    ctx.insert("a", row![2u64])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    let (changes, _) = trace_of(&mut serial, 1);
    assert_eq!(changes[0].0.len(), 2);
    assert_eq!(serial.store().table("a").unwrap().len(), 2);

    let mut parallel = fixture(PartitionConfig::new().with_execution(ExecutionMode::Parallel(2)));
    parallel
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "liar_a",
                10,
                SystemAccess::new(&[], &[]),
                |ctx, _| {
                    ctx.insert("a", row![1u64])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    parallel
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(1),
                "liar_b",
                20,
                SystemAccess::new(&[], &[]),
                |ctx, _| {
                    ctx.insert("a", row![2u64])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    let error = parallel.tick(&frame(0)).unwrap_err();
    assert!(matches!(
        error.error(),
        Error::Internal(message) if message.contains("undeclared write/write dependency")
    ));
    // Zero authoritative mutation: the detection never commits a wrong state.
    assert!(parallel.store().table("a").unwrap().is_empty());
}

#[test]
fn lying_read_declaration_is_detected() {
    // `reader` reads table `a` but does not declare it; `writer` writes it
    // (declared). The planner would group them (reader declares nothing that
    // conflicts), so the merge must detect the read/write overlap.
    let mut parallel = fixture(PartitionConfig::new().with_execution(ExecutionMode::Parallel(2)));
    parallel
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "writer",
                10,
                SystemAccess::new(&[], &["a"]),
                |ctx, _| {
                    ctx.insert("a", row![1u64])?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    parallel
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(1),
                "reader",
                20,
                SystemAccess::new(&[], &[]),
                |ctx, _| {
                    // Reads table a without declaring it.
                    let rows = ctx.scan("a")?;
                    let _ = rows;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    let error = parallel.tick(&frame(0)).unwrap_err();
    assert!(matches!(
        error.error(),
        Error::Internal(message) if message.contains("undeclared read/write dependency")
    ));
    assert!(parallel.store().table("a").unwrap().is_empty());
}

// ------------------------------------------------------- failure semantics

#[test]
fn first_failure_in_system_order_fails_the_tick_identically() {
    let run = |config: PartitionConfig| -> (Error, bool) {
        let mut world = fixture(config);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "writes_first",
                    10,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.insert("a", row![1u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        // Disjoint from the first system (table b) — same group, runs
        // concurrently, and fails.
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(1),
                    "fails_second",
                    20,
                    SystemAccess::new(&[], &["b"]),
                    |_ctx, _| Err(Error::invalid_argument("system rejected the tick")),
                )
                .unwrap(),
            )
            .unwrap();
        let error = world.tick(&frame(0)).unwrap_err();
        let empty = world.store().table("a").unwrap().is_empty();
        (error.error().clone(), empty)
    };

    let (serial_error, serial_empty) =
        run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let (parallel_error, parallel_empty) =
            run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial_error, parallel_error, "Parallel({workers})");
        assert!(
            serial_empty && parallel_empty,
            "zero authoritative mutation"
        );
    }
}

#[test]
fn panicking_system_in_a_parallel_group_fails_atomically() {
    let run = |config: PartitionConfig| -> Error {
        let mut world = fixture(config);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "writes",
                    10,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.insert("a", row![1u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(1),
                    "panics",
                    20,
                    SystemAccess::new(&[], &["b"]),
                    |_ctx, _| panic!("parallel boom"),
                )
                .unwrap(),
            )
            .unwrap();
        let error = world.tick(&frame(0)).unwrap_err();
        assert!(
            world.store().table("a").unwrap().is_empty(),
            "no partial mutation"
        );
        error.error().clone()
    };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    assert!(matches!(&serial, Error::Internal(message) if message.contains("parallel boom")));
    for workers in [1usize, 2, 4] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial, parallel, "Parallel({workers}) panic error diverged");
    }
}

#[test]
fn event_budget_fails_parallel_ticks_deterministically() {
    let config = PartitionConfig::new()
        .with_max_events_per_tick(1)
        .with_execution(ExecutionMode::Parallel(2));
    let mut world = fixture(config);
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(0),
                "emitter_a",
                10,
                SystemAccess::new(&[], &["a"]),
                |ctx, _| {
                    ctx.emit("a", 1u64)?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::with_access(
                SystemId::from_u64(1),
                "emitter_b",
                20,
                SystemAccess::new(&[], &["b"]),
                |ctx, _| {
                    ctx.emit("b", 2u64)?;
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::Capacity(_)));
    assert!(world.store().table("a").unwrap().is_empty());
}

// ------------------------------------------------- reducers and wasm

#[test]
fn native_reducer_invoked_from_a_parallel_child_commits_atomically() {
    let run = |config: PartitionConfig| {
        let mut world = fixture(config);
        world
            .native_mut()
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "spawn", |ctx, args| {
                    let id = args.require_u64("id")?;
                    ctx.insert("a", row![id])?;
                    ctx.emit("spawned", id)?;
                    Ok(Value::U64(id))
                })
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "invoker",
                    0,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        let value =
                            ctx.invoke_reducer("spawn", &ReducerArgs::new().insert("id", 7u64))?;
                        if value != Value::U64(7) {
                            return Err(Error::internal("reducer return mismatch"));
                        }
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        trace_of(&mut world, 1)
    };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial, parallel, "Parallel({workers})");
    }
    // The reducer emitted exactly one event that escaped on commit.
    assert_eq!(serial.0[0].1.len(), 1);
    assert_eq!(serial.0[0].1[0].name(), "spawned");
}

#[test]
fn reducer_failure_inside_a_parallel_child_aborts_the_tick() {
    let run = |config: PartitionConfig| {
        let mut world = fixture(config);
        world
            .native_mut()
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "reject", |_ctx, _| {
                    Err(Error::invalid_argument("rejected"))
                })
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "invoker",
                    0,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.insert("a", row![1u64])?;
                        ctx.invoke_reducer("reject", &ReducerArgs::new())?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let error = world.tick(&frame(0)).unwrap_err();
        let empty = world.store().table("a").unwrap().is_empty();
        (error.error().clone(), empty)
    };

    let (serial_error, serial_empty) =
        run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let (parallel_error, parallel_empty) =
            run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial_error, parallel_error);
        assert!(serial_empty && parallel_empty);
    }
}

/// A minimal WASM module inserting row [42] into table `a` and returning 42.
///
/// The guest builds its op payload at runtime (the host overwrites the input
/// buffer with the encoded args at instantiation, so a static data section
/// there would be clobbered).
fn wasm_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "a")
  (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
    (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
  (func $put_row1 (param $p i32) (param $id i64) (result i32)
    (i64.store align=1 (local.get $p) (i64.const 1))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 8)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 9)) (local.get $id))
    (i32.add (local.get $p) (i32.const 17)))
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))
  (func $ret_u64 (param $v i64) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 8))
    (i64.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 9))
  (func (export "_nexum_reducer_run") (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 1)))
    (local.set $p (call $put_row1 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 42))))"#,
    )
    .expect("valid WAT")
}

#[test]
fn wasm_reducer_invoked_from_a_parallel_child_is_sandboxed_and_atomic() {
    let run = |config: PartitionConfig| {
        let mut world = fixture(config);
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("ping", 1, wasm_module()).unwrap();
        world.set_wasm(wasm);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "wasm_invoker",
                    0,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        let value = ctx.invoke_wasm("ping", &ReducerArgs::new())?;
                        if value != Value::U64(42) {
                            return Err(Error::internal("wasm return mismatch"));
                        }
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        trace_of(&mut world, 1)
    };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial, parallel, "Parallel({workers})");
    }
    // Exactly one committed insert, row [42], real id 0.
    let state = &serial.1;
    let a_rows = &state[0].1;
    assert_eq!(a_rows.len(), 1);
    assert_eq!(a_rows[0].1, row![42u64]);
}

#[test]
fn trapped_wasm_in_a_parallel_child_aborts_atomically() {
    let run = |config: PartitionConfig| {
        let mut world = fixture(config);
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register(
            "explode",
            1,
            wat::parse_str(
                r#"(module
          (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 16)
          (global (export "_nexum_in_ptr") i32 (i32.const 0))
          (global (export "_nexum_out_ptr") i32 (i32.const 16384))
          (func (export "_nexum_reducer_run") (result i32) (unreachable)))"#,
            )
            .unwrap(),
        )
        .unwrap();
        world.set_wasm(wasm);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(0),
                    "writes",
                    10,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        ctx.insert("a", row![1u64])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(1),
                    "traps",
                    20,
                    SystemAccess::new(&[], &["b"]),
                    |ctx, _| {
                        ctx.invoke_wasm("explode", &ReducerArgs::new())?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let error = world.tick(&frame(0)).unwrap_err();
        let empty = world.store().table("a").unwrap().is_empty();
        (error.error().clone(), empty)
    };

    let (serial_error, serial_empty) =
        run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let (parallel_error, parallel_empty) =
            run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial_error, parallel_error);
        assert!(serial_empty && parallel_empty);
    }
}

// ---------------------------------------------------------------- scaling

/// The ten single-column tables used by the scaling test (ids 0..=9).
const WIDE_TABLES: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

/// 100 systems across 10 tables, one shared capture-free closure: system `i`
/// writes table `WIDE_TABLES[i % 10]`, so the greedy planner builds 10
/// groups of 10 pairwise-disjoint members — the maximal real parallelism.
fn wide_world(config: PartitionConfig) -> Partition {
    let mut store = TableStore::new();
    for name in WIDE_TABLES {
        store
            .create_table(
                TableSchema::builder(name)
                    .column("id", ColumnType::U64)
                    .primary_key(&["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
    let mut world = Partition::new(WorldId::from_u64(0), store, config).unwrap();
    for i in 0..100u64 {
        let table = WIDE_TABLES[(i % 10) as usize];
        let access = SystemAccess::new(&[], &[table]);
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(i),
                    format!("sys_{i}"),
                    0,
                    access,
                    // Capture-free: everything derives from the context.
                    |ctx, _| {
                        let id = ctx.system().as_u64();
                        let table = WIDE_TABLES[(id % 10) as usize];
                        ctx.insert(table, row![id * 1000 + ctx.tick().as_u64()])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    world
}

#[test]
fn one_hundred_systems_in_ten_groups_are_worker_count_independent() {
    let run = |config: PartitionConfig| -> Trace { trace_of(&mut wide_world(config), 2) };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4, 8] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(
            serial, parallel,
            "Parallel({workers}) diverged for 100 systems"
        );
    }
    // Sanity: 100 systems x 2 ticks = 200 committed inserts.
    let total: usize = serial.0.iter().map(|(c, _)| c.len()).sum();
    assert_eq!(total, 200);
}
