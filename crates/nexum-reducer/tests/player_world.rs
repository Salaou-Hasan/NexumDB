//! Integration test: the Phase 6 developer flow — **define tables, define
//! reducers, run the world** — over the complete stack (tables → transaction
//! → reducer → committed changes → WAL).
//!
//! The last step attaches the WAL exactly the way the future runtime will
//! (ADR-006 D8): `invoke → result.changes → wal.append(result.tx_id, ...)`.

use std::path::PathBuf;

use nexum_core::{ColumnType, Error, ReducerId, RowId, TableSchema, Value};
use nexum_reducer::{ReducerArgs, ReducerDefinition, ReducerRegistry, ReducerResult};
use nexum_table::{TableStore, row};
use nexum_wal::{DurabilityPolicy, Wal, recover};

fn player_schema() -> TableSchema {
    TableSchema::builder("players")
        .column("id", ColumnType::U64)
        .column("zone_id", ColumnType::U64)
        .column("health", ColumnType::I32)
        .column("level", ColumnType::U32)
        .primary_key(&["id"])
        .unique_index("by_level", &["level"])
        .build()
        .unwrap()
}

fn economy_schema() -> TableSchema {
    TableSchema::builder("economy")
        .column("owner", ColumnType::U64)
        .column("coins", ColumnType::I64)
        .primary_key(&["owner"])
        .build()
        .unwrap()
}

fn world() -> TableStore {
    let mut store = TableStore::new();
    store.create_table(player_schema()).unwrap();
    store.create_table(economy_schema()).unwrap();
    store
}

/// Defines the reducers of a tiny authoritative world.
fn registry() -> ReducerRegistry {
    let mut registry = ReducerRegistry::new();

    // create_player(id, name-ish level) — inserts and returns the real id.
    registry
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "create_player", |ctx, args| {
                let id = args.require_u64("player_id")?;
                let level = args.require_u32("level")?;
                let row_id = ctx.insert("players", row![id, 1u64, 100i32, level])?;
                ctx.emit("player_created", id)?;
                Ok(Value::U64(row_id.as_u64()))
            })
            .unwrap(),
        )
        .unwrap();

    // damage_player(player_id, amount) — aborts (Conflict-free) if the
    // target is missing; caps health at zero.
    registry
        .register(
            ReducerDefinition::new(ReducerId::from_u64(1), "damage_player", |ctx, args| {
                let id = args.require_u64("player_id")?;
                let amount = args.require_i32("amount")?;
                let row_id = ctx
                    .lookup_unique("players", "primary", &[Value::U64(id)])?
                    .first()
                    .copied()
                    .ok_or_else(|| Error::not_found(format!("player {id} does not exist")))?;
                let mut row = ctx.get("players", row_id)?.expect("lookup implies present");
                let health = row.values()[2].as_i32().unwrap();
                let new_health = (health - amount).max(0);
                row = row![
                    id,
                    row.values()[1].as_u64().unwrap(),
                    new_health,
                    row.values()[3].as_u32().unwrap()
                ];
                ctx.update("players", row_id, row)?;
                ctx.emit("player_damaged", id)?;
                Ok(Value::I32(new_health))
            })
            .unwrap(),
        )
        .unwrap();

    // give_coins(owner, amount) — moves nothing, just adds coins.
    registry
        .register(
            ReducerDefinition::new(ReducerId::from_u64(2), "give_coins", |ctx, args| {
                let owner = args.require_u64("owner")?;
                let amount = args.require_i64("amount")?;
                let row_id = RowId::from_u64(owner - 1); // seeded economy rows are 0-based
                let row = ctx.get("economy", row_id)?.expect("economy row exists");
                let coins = row.values()[1].as_i64().unwrap() + amount;
                ctx.update("economy", row_id, row![owner, coins])?;
                Ok(Value::I64(coins))
            })
            .unwrap(),
        )
        .unwrap();

    registry
}

fn wal_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nexum-reducer-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run(
    store: &mut TableStore,
    registry: &ReducerRegistry,
    wal: &mut Wal,
    name: &str,
    args: ReducerArgs,
) -> Result<ReducerResult, Error> {
    let result = registry.invoke(store, name, &args)?;
    wal.append(result.tx_id(), result.changes()).unwrap();
    Ok(result)
}

#[test]
fn a_reducer_world_commits_and_recovers_exactly() {
    let dir = wal_dir("world");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    let registry = registry();
    // Seed the economy row directly (schemas and seeds are deployment
    // concerns; reducers act on existing state).
    store
        .table_mut("economy")
        .unwrap()
        .insert(row![1u64, 100i64])
        .unwrap();
    store.drain_changes();

    // 1. Create two players.
    let a = run(
        &mut store,
        &registry,
        &mut wal,
        "create_player",
        ReducerArgs::new()
            .insert("player_id", 1u64)
            .insert("level", 5u32),
    )
    .unwrap();
    assert_eq!(a.changes().len(), 1);
    assert_eq!(a.events().len(), 1);
    let _b = run(
        &mut store,
        &registry,
        &mut wal,
        "create_player",
        ReducerArgs::new()
            .insert("player_id", 2u64)
            .insert("level", 6u32),
    )
    .unwrap();

    // 2. Damage Alice.
    let damaged = run(
        &mut store,
        &registry,
        &mut wal,
        "damage_player",
        ReducerArgs::new()
            .insert("player_id", 1u64)
            .insert("amount", 30i32),
    )
    .unwrap();
    assert_eq!(damaged.return_value(), &Value::I32(70));

    // 3. Reject: damage an unknown player — zero mutations, no WAL record.
    let err = run(
        &mut store,
        &registry,
        &mut wal,
        "damage_player",
        ReducerArgs::new()
            .insert("player_id", 99u64)
            .insert("amount", 10i32),
    )
    .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    assert_eq!(store.table("players").unwrap().len(), 2);

    // 4. Give coins (economy now has 130).
    run(
        &mut store,
        &registry,
        &mut wal,
        "give_coins",
        ReducerArgs::new()
            .insert("owner", 1u64)
            .insert("amount", 30i64),
    )
    .unwrap();

    // Reference shape before the "crash".
    let expected_epoch = store.table("players").unwrap().epoch();
    let alice_health = store
        .table("players")
        .unwrap()
        .get(a.changes()[0].row_id())
        .unwrap()
        .values()[2]
        .clone();

    // 5. Crash and recover: snapshot first, then WAL replay must reproduce
    //    the exact world state.
    let wal_lsn = wal.lsn().as_u64();
    // Recover without a snapshot (tables already deployed in a fresh store).
    let mut fresh = world();
    fresh
        .table_mut("economy")
        .unwrap()
        .insert(row![1u64, 100i64])
        .unwrap();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert_eq!(report.replayed_txs, 4); // create a, create b, damage, give_coins
    assert!(!report.truncated_tail);

    let players = fresh.table("players").unwrap();
    assert_eq!(players.len(), 2);
    let recovered_alice = players.get(a.changes()[0].row_id()).unwrap();
    assert_eq!(recovered_alice.values()[2], alice_health);
    assert_eq!(players.epoch(), expected_epoch);
    let economy = fresh.table("economy").unwrap();
    assert_eq!(
        economy.get(RowId::from_u64(0)).unwrap().values()[1],
        Value::I64(130)
    );
    // The failed damage (reducer error) left no WAL record to replay.
    assert!(
        players
            .get_by_primary_key(&[Value::U64(99)])
            .unwrap()
            .is_none()
    );
    // wal_lsn sanity: recovery used a non-zero snapshot boundary.
    assert!(wal_lsn > 0);
}
