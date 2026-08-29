//! Phase 12 unit tests (ADR-012): cross-partition messaging at the world
//! level — `send_to` validation, committed outbound, deterministic delivery
//! ordering, native/WASM handlers, failure atomicity, and serial/parallel
//! outbound-trace equality.

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, Error, PartitionId, Row, RowId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use crate::input::InputFrame;
use crate::partition::PartitionMessage;
use crate::systems::{SystemAccess, SystemDefinition};
use crate::{ExecutionMode, Partition, PartitionConfig};

/// A world with tables `a` and `ledger`, partition id 7, topology {1, 7, 9}.
fn fixture(config: PartitionConfig) -> Partition {
    let mut store = TableStore::new();
    for (name, cols) in [
        ("a", vec![("id", ColumnType::U64)]),
        (
            "ledger",
            vec![
                ("id", ColumnType::U64),
                ("from", ColumnType::U64),
                ("seq", ColumnType::U64),
            ],
        ),
    ] {
        let mut builder = TableSchema::builder(name).column(cols[0].0, cols[0].1);
        for &(column, ty) in &cols[1..] {
            builder = builder.column(column, ty);
        }
        store
            .create_table(builder.primary_key(&["id"]).build().unwrap())
            .unwrap();
    }
    let mut world = Partition::new(WorldId::from_u64(0), store, config).unwrap();
    world.set_partition(PartitionId::from_u64(7));
    world.set_known_partitions(vec![
        PartitionId::from_u64(9),
        PartitionId::from_u64(7),
        PartitionId::from_u64(1),
    ]);
    world
}

fn frame(tick: u64) -> InputFrame {
    InputFrame::new(TickId::from_u64(tick))
}

fn msg(from: u64, to: u64, sent_tick: u64, seq: u64, kind: &str) -> PartitionMessage {
    PartitionMessage::new(
        PartitionId::from_u64(from),
        PartitionId::from_u64(to),
        TickId::from_u64(sent_tick),
        seq,
        kind.to_string(),
        // The payload mirrors the envelope (from, seq) so the `record`
        // handler can assert delivery order from inside the transaction.
        ReducerArgs::new().insert("from", from).insert("seq", seq),
    )
    .unwrap()
}

// -------------------------------------------------------- send_to validation

#[test]
fn send_to_validates_target_kind_and_budget() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                // Unknown target (not in topology {1, 7, 9}).
                let err = ctx
                    .send_to(PartitionId::from_u64(42), "move", ReducerArgs::new())
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)));
                // Sending to self is rejected.
                let err = ctx
                    .send_to(PartitionId::from_u64(7), "move", ReducerArgs::new())
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)));
                // Empty and oversized kinds are rejected.
                let err = ctx
                    .send_to(PartitionId::from_u64(1), "", ReducerArgs::new())
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)));
                let long = "x".repeat(257);
                let err = ctx
                    .send_to(PartitionId::from_u64(1), &long, ReducerArgs::new())
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)));
                // Oversized payload is rejected.
                let mut args = ReducerArgs::new();
                for i in 0..10_001 {
                    args = args.insert(format!("k{i}"), i);
                }
                let err = ctx
                    .send_to(PartitionId::from_u64(1), "move", args)
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)));
                // A valid send succeeds.
                ctx.send_to(
                    PartitionId::from_u64(1),
                    "move",
                    ReducerArgs::new().insert("n", 1u64),
                )?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let result = world.tick(&frame(0)).expect("tick committed");
    assert_eq!(result.outbound().len(), 1);
    assert_eq!(result.outbound()[0].to(), PartitionId::from_u64(1));
}

#[test]
fn outbound_budget_fails_the_tick_atomically() {
    let config = PartitionConfig::new().with_max_messages_per_tick(1);
    let mut world = fixture(config);
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                ctx.send_to(PartitionId::from_u64(1), "a", ReducerArgs::new())?;
                ctx.send_to(PartitionId::from_u64(9), "b", ReducerArgs::new())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::Capacity(_)));
}

// ----------------------------------------------------------- committed outbound

#[test]
fn outbound_commits_with_the_tick_in_send_order() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                // Also mutate authoritative state: a failed tick must discard
                // both writes and messages together.
                ctx.insert("a", row![1u64])?;
                ctx.send_to(
                    PartitionId::from_u64(1),
                    "first",
                    ReducerArgs::new().insert("n", 1u64),
                )?;
                ctx.send_to(
                    PartitionId::from_u64(9),
                    "second",
                    ReducerArgs::new().insert("n", 2u64),
                )?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let result = world.tick(&frame(0)).expect("tick committed");
    let outbound = result.outbound();
    assert_eq!(outbound.len(), 2);
    assert_eq!(outbound[0].kind(), "first");
    assert_eq!(outbound[0].seq(), 0);
    assert_eq!(outbound[1].kind(), "second");
    assert_eq!(outbound[1].seq(), 1);
    for message in outbound {
        assert_eq!(message.from(), PartitionId::from_u64(7));
        assert_eq!(message.sent_tick(), TickId::from_u64(0));
    }
    assert_eq!(outbound[0].to(), PartitionId::from_u64(1));
    assert_eq!(outbound[1].to(), PartitionId::from_u64(9));
    assert_eq!(outbound[0].payload().require_u64("n").unwrap(), 1);
}

#[test]
fn failed_tick_discards_outbound_messages() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                ctx.send_to(PartitionId::from_u64(1), "move", ReducerArgs::new())?;
                Err(Error::invalid_argument("tick rejects after sending"))
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    assert!(world.store().table("a").unwrap().is_empty());
}

// ------------------------------------------------------------- delivery order

/// A world whose message handler records `(from, seq)` rows into the ledger
/// (id = from * 1000 + seq, unique across the test batch), so tests can
/// assert exact delivery order. The handler writes through the tick's
/// transaction, so it commits atomically with the tick.
fn handler_world(config: PartitionConfig) -> Partition {
    let mut world = fixture(config);
    world
        .native_mut()
        .register(
            ReducerDefinition::new(nexum_core::ReducerId::from_u64(0), "record", |ctx, args| {
                let from = args.require_u64("from")?;
                let seq = args.require_u64("seq")?;
                ctx.insert("ledger", row![from * 1000 + seq, from, seq])?;
                Ok(Value::U64(seq))
            })
            .unwrap(),
        )
        .unwrap();
    world
}

#[test]
fn delivered_messages_run_handlers_in_deterministic_batch_order() {
    let mut world = handler_world(PartitionConfig::new());
    // Delivered out of (sent_tick, from, seq) order on purpose: the world
    // must sort deterministically.
    let delivered = vec![
        msg(2, 7, 0, 1, "record"),
        msg(1, 7, 0, 3, "record"),
        msg(1, 7, 0, 2, "record"),
        msg(1, 7, 0, 1, "record"),
        msg(1, 7, 1, 0, "record"),
    ];
    let result = world
        .tick_messages(&frame(0), &delivered)
        .expect("tick committed");
    // Handlers wrote one ledger row per message; order follows the sort.
    let rows: Vec<(RowId, Row)> = world
        .store()
        .table("ledger")
        .unwrap()
        .scan()
        .map(|(id, r)| (id, r.clone()))
        .collect();
    assert_eq!(rows.len(), 5);
    // (sent_tick, from, seq) sort of the delivered batch:
    //   (0,1,1) (0,1,2) (0,1,3) (0,2,1) (1,1,0)
    // → recorded (from, seq) pairs in delivery order:
    //   (1,1) (1,2) (1,3) (2,1) (1,0)
    let keys: Vec<(u64, u64)> = rows
        .iter()
        .map(|(_, r)| {
            (
                r.get(1).unwrap().as_u64().unwrap(),
                r.get(2).unwrap().as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(keys, vec![(1, 1), (1, 2), (1, 3), (2, 1), (1, 0)]);
    assert_eq!(result.outbound().len(), 0);
}

// --------------------------------------------------------- handler failures

#[test]
fn unhandled_message_kind_fails_the_tick_atomically() {
    let mut world = handler_world(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                ctx.insert("a", row![1u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let delivered = vec![msg(1, 7, 0, 0, "nobody_handles_this")];
    let error = world.tick_messages(&frame(0), &delivered).unwrap_err();
    assert!(matches!(error.error(), Error::NotFound(_)));
    assert!(world.store().table("a").unwrap().is_empty());
    assert!(world.store().table("ledger").unwrap().is_empty());
}

#[test]
fn rejecting_handler_fails_the_tick_atomically() {
    let mut world = fixture(PartitionConfig::new());
    world
        .native_mut()
        .register(
            ReducerDefinition::new(nexum_core::ReducerId::from_u64(0), "reject", |_ctx, _| {
                Err(Error::invalid_argument("handler rejected the message"))
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                ctx.insert("a", row![1u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let delivered = vec![msg(1, 7, 0, 0, "reject")];
    let error = world.tick_messages(&frame(0), &delivered).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    assert!(world.store().table("a").unwrap().is_empty());
}

#[test]
fn misdirected_delivery_is_rejected() {
    let mut world = fixture(PartitionConfig::new());
    // Partition 7 must reject a message addressed to partition 1.
    let delivered = vec![msg(9, 1, 0, 0, "record")];
    let error = world.tick_messages(&frame(0), &delivered).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    assert_eq!(world.tick_number(), TickId::from_u64(0));
}

// -------------------------------------------------------------- wasm handler

fn wasm_handler_module() -> Vec<u8> {
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
fn wasm_message_handlers_commit_atomically() {
    let mut world = fixture(PartitionConfig::new());
    let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    wasm.register("ping", 1, wasm_handler_module()).unwrap();
    world.set_wasm(wasm);
    // Register a native handler that would fail if invoked, proving the
    // wasm fallback is used only after the native registry misses.
    world
        .native_mut()
        .register(
            ReducerDefinition::new(
                nexum_core::ReducerId::from_u64(9),
                "unrelated",
                |_ctx, _| Err(Error::internal("wrong handler invoked")),
            )
            .unwrap(),
        )
        .unwrap();

    let delivered = vec![msg(1, 7, 0, 0, "ping")];
    let result = world
        .tick_messages(&frame(0), &delivered)
        .expect("tick committed");
    assert_eq!(result.changes().len(), 1);
    let rows: Vec<Row> = world
        .store()
        .table("a")
        .unwrap()
        .scan()
        .map(|(_, r)| r.clone())
        .collect();
    assert_eq!(rows, vec![row![42u64]]);
}

// -------------------------------------------- serial/parallel outbound parity

#[test]
fn parallel_mode_produces_identical_outbound_traces() {
    let run = |config: PartitionConfig| -> Vec<PartitionMessage> {
        let mut world = fixture(config);
        for id in 0..3u64 {
            world
                .add_system(
                    SystemDefinition::with_access(
                        SystemId::from_u64(id),
                        format!("sender_{id}"),
                        id as u32,
                        SystemAccess::new(&[], &[]),
                        // Capture-free: everything derives from the context.
                        |ctx, _| {
                            let id = ctx.system().as_u64();
                            let kind = match id {
                                0 => "a",
                                1 => "b",
                                _ => "c",
                            };
                            ctx.send_to(
                                PartitionId::from_u64(1),
                                kind,
                                ReducerArgs::new().insert("n", id),
                            )?;
                            Ok(())
                        },
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        world
            .tick(&frame(0))
            .expect("tick committed")
            .outbound()
            .to_vec()
    };

    let serial = run(PartitionConfig::new().with_execution(ExecutionMode::Serial));
    for workers in [1usize, 2, 4] {
        let parallel = run(PartitionConfig::new().with_execution(ExecutionMode::Parallel(workers)));
        assert_eq!(serial, parallel, "Parallel({workers}) outbound diverged");
    }
    // Kind order follows system order (a, b, c) with global seqs 0,1,2.
    assert_eq!(serial[0].kind(), "a");
    assert_eq!(serial[1].kind(), "b");
    assert_eq!(serial[2].kind(), "c");
    assert_eq!(serial[0].seq(), 0);
    assert_eq!(serial[2].seq(), 2);
}

#[test]
fn tick_delegates_with_an_empty_batch() {
    let mut world = handler_world(PartitionConfig::new());
    let result = world.tick(&frame(0)).expect("tick committed");
    assert!(result.outbound().is_empty());
    assert!(world.store().table("ledger").unwrap().is_empty());
}
