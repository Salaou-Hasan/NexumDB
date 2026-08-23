//! Reducer tests: registration, invocation, reads/writes, read-your-writes,
//! atomicity, events, conflicts, multi-table, panics, registry, determinism.
//!
//! Genuine OCC conflicts are tested at the boundary `invoke` uses (begin →
//! context → interleaved committed write → commit): a single-threaded
//! exclusive-ownership model serializes writers, so an `invoke` call itself
//! cannot race an external transaction (ADR-004 D10). The reducer layer's
//! contract — propagate `Error::Conflict` unchanged with zero mutation — is
//! verified directly at that boundary.
//!
//! Reducer closures are `fn` pointers, so tests pass any row ids they need
//! through [`ReducerArgs`] rather than capturing them.

use nexum_core::{ColumnType, Error, ReducerId, RowId, TableSchema, TransactionId, Value};
use nexum_table::{TableStore, row};
use nexum_tx::Transaction;

use crate::ReducerFn;
use crate::args::ReducerArgs;
use crate::context::ReducerContext;
use crate::definition::ReducerDefinition;
use crate::registry::ReducerRegistry;

/// Two tables: `players` (with a secondary index and a unique index) and
/// `economy`. Players columns: `[id, zone_id, health, level]` — `health` is
/// values index 2. Economy columns: `[owner, coins]` — `coins` is index 1.
fn world() -> TableStore {
    let mut store = TableStore::new();
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .column("health", ColumnType::I32)
                .column("level", ColumnType::U32)
                .primary_key(&["id"])
                .index("by_zone", &["zone_id"])
                .unique_index("by_level", &["level"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
        .create_table(
            TableSchema::builder("economy")
                .column("owner", ColumnType::U64)
                .column("coins", ColumnType::I64)
                .primary_key(&["owner"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

/// Seeds Alice (zone 10, level 5) and Bob (zone 20, level 6) plus one
/// economy row; drains change buffers.
fn seeded() -> (TableStore, RowId, RowId) {
    let mut store = world();
    let alice = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let bob = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 20u64, 90i32, 6u32])
        .unwrap();
    store
        .table_mut("economy")
        .unwrap()
        .insert(row![1u64, 100i64])
        .unwrap();
    store.drain_changes();
    (store, alice, bob)
}

fn register(registry: &mut ReducerRegistry, id: u64, name: &str, execute: ReducerFn) {
    registry
        .register(ReducerDefinition::new(ReducerId::from_u64(id), name, execute).unwrap())
        .unwrap();
}

fn health(row: &nexum_core::Row) -> &Value {
    &row.values()[2]
}

fn coins(row: &nexum_core::Row) -> &Value {
    &row.values()[1]
}

// ------------------------------------------------------------- basic

#[test]
fn invoke_runs_a_reducer_and_returns_its_value() {
    let mut store = world();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "ping", |_ctx, _args| {
        Ok(Value::String("pong".into()))
    });

    let result = registry
        .invoke(&mut store, "ping", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::String("pong".into()));
    assert!(result.changes().is_empty());
    assert!(result.events().is_empty());
    assert_eq!(result.tx_id(), TransactionId::from_u64(0));
}

#[test]
fn invoke_accepts_and_returns_arguments() {
    let mut store = world();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "echo", |_ctx, args| {
        Ok(args.require_u64("value")?.into())
    });

    let args = ReducerArgs::new().insert("value", 42u64);
    let result = registry.invoke(&mut store, "echo", &args).unwrap();
    assert_eq!(result.return_value(), &Value::U64(42));
}

#[test]
fn invoke_unknown_reducer_is_not_found() {
    let mut store = world();
    let registry = ReducerRegistry::new();
    let err = registry
        .invoke(&mut store, "nope", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

// ------------------------------------------------------------- registry

#[test]
fn registry_rejects_duplicate_id_and_duplicate_name() {
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "a", |_ctx, _args| Ok(Value::U64(0)));

    let same_id =
        ReducerDefinition::new(ReducerId::from_u64(0), "b", |_ctx, _args| Ok(Value::U64(0)))
            .unwrap();
    assert!(matches!(
        registry.register(same_id),
        Err(Error::AlreadyExists(_))
    ));

    let same_name =
        ReducerDefinition::new(ReducerId::from_u64(1), "a", |_ctx, _args| Ok(Value::U64(0)))
            .unwrap();
    assert!(matches!(
        registry.register(same_name),
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(registry.len(), 1);
}

#[test]
fn registry_lists_deterministically_in_ascending_id_order() {
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 2, "zeta", |_ctx, _args| Ok(Value::U64(2)));
    register(&mut registry, 0, "alpha", |_ctx, _args| Ok(Value::U64(0)));
    register(&mut registry, 1, "mid", |_ctx, _args| Ok(Value::U64(1)));

    let names: Vec<&str> = registry.list().map(ReducerDefinition::name).collect();
    assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    assert!(registry.contains("alpha"));
    assert!(registry.contains_id(ReducerId::from_u64(2)));
    assert!(registry.lookup(ReducerId::from_u64(1)).is_some());
    assert!(registry.lookup_by_name("zeta").is_some());
    assert!(registry.lookup_by_name("nope").is_none());
}

#[test]
fn reducer_name_must_not_be_empty() {
    let err = ReducerDefinition::new(ReducerId::from_u64(0), "", |_ctx, _args| Ok(Value::U64(0)))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

// ------------------------------------------------------------- reads + writes

#[test]
fn reducer_reads_writes_and_scans() {
    let (mut store, alice, bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "patrol", |ctx, args| {
        let alice = RowId::from_u64(args.require_u64("alice")?);
        let bob = RowId::from_u64(args.require_u64("bob")?);

        // Reads: point get, set scan, and a unique-key lookup.
        let seen = ctx.scan("players")?;
        let alice_row = ctx.get("players", alice)?.expect("alice exists");
        let level_six = ctx.lookup_unique("players", "by_level", &[Value::U32(6)])?;
        assert_eq!(seen.len(), 2);
        assert_eq!(health(&alice_row), &Value::I32(100));
        assert_eq!(level_six, vec![bob]);

        // Writes: insert, update, delete across two tables.
        let carol = ctx.insert("players", row![3u64, 10u64, 80i32, 7u32])?;
        ctx.update("players", alice, row![1u64, 30u64, 50i32, 5u32])?;
        ctx.delete("economy", RowId::from_u64(0))?;
        Ok(Value::U64(carol.as_u64()))
    });

    let args = ReducerArgs::new()
        .insert("alice", alice.as_u64())
        .insert("bob", bob.as_u64());
    let result = registry.invoke(&mut store, "patrol", &args).unwrap();
    assert_eq!(result.changes().len(), 3); // insert + update + delete
    assert_eq!(store.table("players").unwrap().len(), 3);
    assert!(store.table("economy").unwrap().is_empty());
    // The by_zone index followed the update (Alice moved to zone 30).
    let players = store.table("players").unwrap();
    assert_eq!(
        players.lookup("by_zone", &[Value::U64(30)]).unwrap(),
        vec![alice]
    );
    assert_eq!(
        players.lookup("by_zone", &[Value::U64(20)]).unwrap(),
        vec![bob]
    );
}

#[test]
fn read_your_writes_through_the_context() {
    let (mut store, alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "ryw", |ctx, args| {
        let alice = RowId::from_u64(args.require_u64("alice")?);
        // insert → get sees the provisional row.
        let handle = ctx.insert("players", row![9u64, 40u64, 10i32, 9u32])?;
        let pending = ctx.get("players", handle)?.expect("pending insert visible");
        assert_eq!(health(&pending), &Value::I32(10));
        // update → get sees the updated value.
        ctx.update("players", handle, row![9u64, 40u64, 5i32, 9u32])?;
        let updated = ctx.get("players", handle)?.expect("updated row visible");
        assert_eq!(health(&updated), &Value::I32(5));
        // delete → get is absent from the transaction view.
        ctx.delete("players", handle)?;
        assert!(ctx.get("players", handle)?.is_none());

        // Same overlay on a real row: update then read the new value.
        ctx.update("players", alice, row![1u64, 30u64, 25i32, 5u32])?;
        let moved = ctx.get("players", alice)?.expect("alice visible");
        assert_eq!(health(&moved), &Value::I32(25));
        Ok(Value::U64(0))
    });

    let args = ReducerArgs::new().insert("alice", alice.as_u64());
    let result = registry.invoke(&mut store, "ryw", &args).unwrap();
    // insert→update→delete netted to nothing; only alice's update commits.
    assert_eq!(result.changes().len(), 1);
    assert_eq!(store.table("players").unwrap().len(), 2);
}

#[test]
fn contains_and_has_table_through_the_context() {
    let (mut store, alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "check", |ctx, args| {
        assert!(ctx.has_table("players"));
        assert!(!ctx.has_table("nope"));
        assert!(ctx.contains("players", RowId::from_u64(args.require_u64("alice")?))?);
        assert!(!ctx.contains("players", RowId::from_u64(99))?);
        Ok(Value::Bool(true))
    });

    let args = ReducerArgs::new().insert("alice", alice.as_u64());
    let result = registry.invoke(&mut store, "check", &args).unwrap();
    assert_eq!(result.return_value(), &Value::Bool(true));
}

// ------------------------------------------------------------- atomicity

#[test]
fn reducer_error_aborts_with_zero_mutations_and_no_events() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "flaky", |ctx, _args| {
        ctx.insert("players", row![9u64, 40u64, 10i32, 9u32])?;
        ctx.emit("should_not_escape", 1u64)?;
        ctx.delete("economy", RowId::from_u64(0))?;
        Err(Error::invalid_argument("rejected by application logic"))
    });

    let err = registry
        .invoke(&mut store, "flaky", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    // Nothing changed: no insert, no delete, no committed events.
    assert_eq!(store.table("players").unwrap().len(), 2);
    assert_eq!(store.table("economy").unwrap().len(), 1);
    assert!(store.drain_changes().is_empty());
}

#[test]
fn unique_key_violation_aborts_with_zero_mutations() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "cheat", |ctx, _args| {
        // Bob already owns level 6 (unique index by_level).
        ctx.insert("players", row![9u64, 40u64, 10i32, 6u32])?;
        Ok(Value::U64(0))
    });

    let err = registry
        .invoke(&mut store, "cheat", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(store.table("players").unwrap().len(), 2);
}

#[test]
fn multi_table_reducer_commits_atomically() {
    let (mut store, _alice, _bob) = seeded();
    store
        .table_mut("economy")
        .unwrap()
        .insert(row![2u64, 50i64])
        .unwrap();
    store.drain_changes();

    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "trade", |ctx, args| {
        let from = RowId::from_u64(args.require_u64("from")?);
        let to = RowId::from_u64(args.require_u64("to")?);
        let amount = args.require_i64("amount")?;
        // Move coins from `from` to `to` in one atomic transaction, and also
        // record the trade in the players table. Row ids are
        // storage-assigned; owners are their own column values.
        let from_row = ctx.get("economy", from)?.expect("sender");
        let to_row = ctx.get("economy", to)?.expect("receiver");
        let from_owner = from_row.values()[0].as_u64().unwrap();
        let to_owner = to_row.values()[0].as_u64().unwrap();
        let from_coins = coins(&from_row).as_i64().unwrap();
        let to_coins = coins(&to_row).as_i64().unwrap();
        ctx.insert("players", row![9u64, 40u64, 1i32, 9u32])?;
        ctx.update("economy", from, row![from_owner, from_coins - amount])?;
        ctx.update("economy", to, row![to_owner, to_coins + amount])?;
        ctx.emit("trade", amount)?;
        Ok(Value::I64(from_coins - amount))
    });

    let args = ReducerArgs::new()
        .insert("from", 0u64)
        .insert("to", 1u64)
        .insert("amount", 30i64);
    let result = registry.invoke(&mut store, "trade", &args).unwrap();

    // All three writes committed atomically, in deterministic order: the
    // players insert (table id 0) before the two economy updates (table id
    // 1), and the economy updates in ascending row-id order.
    assert_eq!(result.changes().len(), 3);
    assert_eq!(
        result.changes()[0].table_id(),
        nexum_core::TableId::from_u64(0)
    );
    assert_eq!(result.changes()[0].kind(), nexum_core::ChangeKind::Insert);
    assert_eq!(
        result.changes()[1].table_id(),
        nexum_core::TableId::from_u64(1)
    );
    assert_eq!(
        result.changes()[2].table_id(),
        nexum_core::TableId::from_u64(1)
    );
    assert!(result.changes()[1].row_id() < result.changes()[2].row_id());

    let economy = store.table("economy").unwrap();
    assert_eq!(
        coins(economy.get(RowId::from_u64(0)).unwrap()),
        &Value::I64(70)
    );
    assert_eq!(
        coins(economy.get(RowId::from_u64(1)).unwrap()),
        &Value::I64(80)
    );
    assert_eq!(result.events().len(), 1);
}

// ------------------------------------------------------------- events

#[test]
fn events_are_preserved_on_commit_in_emit_order() {
    let mut store = world();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "story", |ctx, _args| {
        ctx.emit("first", 1u64)?;
        ctx.emit("second", "two")?;
        ctx.emit("third", 3u64)?;
        Ok(Value::U64(0))
    });

    let result = registry
        .invoke(&mut store, "story", &ReducerArgs::new())
        .unwrap();
    let names: Vec<&str> = result.events().iter().map(|e| e.name()).collect();
    assert_eq!(names, vec!["first", "second", "third"]);
    assert_eq!(result.events()[1].payload(), &Value::String("two".into()));
}

#[test]
fn empty_event_name_is_invalid_argument() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "bad_event", |ctx, _args| {
        ctx.insert("players", row![9u64, 40u64, 10i32, 9u32])?;
        ctx.emit("", 1u64)?;
        Ok(Value::U64(0))
    });

    let err = registry
        .invoke(&mut store, "bad_event", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(
        store.table("players").unwrap().len(),
        2,
        "the insert aborted with the event"
    );
}

// ------------------------------------------------------------- conflicts

#[test]
fn point_read_conflicts_with_a_committed_write() {
    // The exact OCC scenario, driven at the boundary `invoke` uses: a
    // context observation invalidated by an external committed write must
    // fail validation with `Error::Conflict` and zero mutation.
    let (mut store, alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.get("players", alice).unwrap();
    }
    // External writer commits a change to the same row.
    store
        .table_mut("players")
        .unwrap()
        .update(alice, row![1u64, 10u64, 42i32, 5u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn scan_epoch_observation_conflicts_with_any_mutation() {
    // A scan records a table-epoch observation: an external insert between
    // observation and commit is a phantom conflict (ADR-004 D13).
    let (mut store, _alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.scan("players").unwrap();
    }
    store
        .table_mut("players")
        .unwrap()
        .insert(row![9u64, 40u64, 10i32, 9u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn version_capture_detects_lost_updates_without_explicit_read() {
    // A write-only reducer still captures the row's version at write time
    // (ADR-004 D12): an external commit before the reducer's own commit is a
    // conflict even though the reducer never called `get`.
    let (mut store, alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.update("players", alice, row![1u64, 10u64, 5i32, 5u32])
            .unwrap();
    }
    store
        .table_mut("players")
        .unwrap()
        .update(alice, row![1u64, 10u64, 42i32, 5u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn unrelated_table_changes_do_not_conflict_a_point_read() {
    // Precision check (Phase 4 correction): a reducer that only point-reads
    // players must not conflict merely because the economy table changed.
    let (mut store, alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.get("players", alice).unwrap();
    }
    store
        .table_mut("economy")
        .unwrap()
        .update(RowId::from_u64(0), row![1u64, 999i64])
        .unwrap();
    store.drain_changes();

    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty(), "no conflict for unrelated tables");
}

// ------------------------------------------------------------- panic

#[test]
fn panic_aborts_with_zero_mutation_and_no_events() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "explode", |ctx, _args| {
        ctx.insert("players", row![9u64, 40u64, 10i32, 9u32])?;
        ctx.emit("should_not_escape", 1u64)?;
        panic!("boom");
    });

    let err = registry
        .invoke(&mut store, "explode", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::Internal(_)));
    assert!(err.to_string().contains("explode"));
    // Zero authoritative mutation, zero events, zero committed changes.
    assert_eq!(store.table("players").unwrap().len(), 2);
    assert!(store.drain_changes().is_empty());

    // The store remains fully usable after a panic.
    let mut registry2 = ReducerRegistry::new();
    register(&mut registry2, 1, "ok", |ctx, _args| {
        ctx.insert("players", row![9u64, 40u64, 10i32, 9u32])
            .map(|id| Value::U64(id.as_u64()))
    });
    let result = registry2
        .invoke(&mut store, "ok", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.changes().len(), 1);
}

// ------------------------------------------------------------- determinism

#[test]
fn same_state_and_args_produce_identical_results() {
    let run = || {
        let (mut store, _alice, _bob) = seeded();
        let mut registry = ReducerRegistry::new();
        register(&mut registry, 0, "spawn", |ctx, args| {
            let id = args.require_u64("id")?;
            let row_id = ctx.insert("players", row![id, 10u64, 100i32, id as u32])?;
            ctx.emit("spawned", id)?;
            Ok(Value::U64(row_id.as_u64()))
        });
        let args = ReducerArgs::new().insert("id", 42u64);
        registry.invoke(&mut store, "spawn", &args).unwrap()
    };

    let first = run();
    let second = run();
    assert_eq!(first.return_value(), second.return_value());
    assert_eq!(first.changes(), second.changes());
    assert_eq!(first.events(), second.events());
    // Each run begins its own fresh store, so the first transaction of each
    // has the same id — the whole result shape is deterministic.
    assert_eq!(first.tx_id(), second.tx_id());
}

#[test]
fn committed_changes_carry_real_row_ids() {
    // The provisional handle returned by `insert` is replaced by the real id
    // storage assigned at commit — the result never leaks provisional ids.
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "spawn", |ctx, args| {
        let id = args.require_u64("id")?;
        let row_id = ctx.insert("players", row![id, 10u64, 100i32, id as u32])?;
        assert_eq!(row_id.as_u64(), 1 << 63, "the handle is provisional");
        Ok(Value::U64(0))
    });
    let args = ReducerArgs::new().insert("id", 42u64);
    let result = registry.invoke(&mut store, "spawn", &args).unwrap();
    assert_eq!(
        result.changes()[0].row_id(),
        RowId::from_u64(2),
        "real storage id"
    );
}

// ------------------------------------------------------------- errors

#[test]
fn reducer_unknown_table_is_not_found_and_aborts() {
    let mut store = world();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "ghost", |ctx, _args| {
        let _ = ctx.get("nope", RowId::from_u64(0))?;
        Ok(Value::U64(0))
    });
    let err = registry
        .invoke(&mut store, "ghost", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[test]
fn missing_row_update_is_not_found_and_aborts() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = ReducerRegistry::new();
    register(&mut registry, 0, "poke", |ctx, _args| {
        ctx.update(
            "players",
            RowId::from_u64(99),
            row![99u64, 10u64, 1i32, 1u32],
        )?;
        Ok(Value::U64(0))
    });
    let err = registry
        .invoke(&mut store, "poke", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}
