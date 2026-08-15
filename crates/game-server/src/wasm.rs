//! The `fire_weapon` reducer as a **WASM module** (ADR-007): the whole
//! combat decision — validate the shooter, compute the aim cell from the
//! authoritative facing, detect the target, apply damage, consume the shot,
//! and emit `hit`/`kill` events — runs inside the sandbox against the tick
//! transaction through the single `("nexum","op")` host function.
//!
//! The module reads exactly one argument: the gateway-stamped `__caller`
//! (the authenticated shooter). It never trusts client-supplied identity or
//! hit data — the client cannot choose its target, its damage, or its own
//! position; the server's authoritative state and this sandboxed logic
//! decide.
//!
//! Phase 17 hot-path discipline: the module uses the derived indexes instead
//! of full-table scans —
//!
//! - the shooter row: `OP_LOOKUP_UNIQUE` (`"primary"`, caller) → row id →
//!   `OP_GET` the row (O(log N));
//! - the target: `OP_LOOKUP_INDEX` (`"pos"`, aim cell) → candidate row ids →
//!   `OP_GET` each, pick the alive non-self one (O(log N + k)).
//!
//! Wire formats (from `nexum-wasm`):
//!
//! - reducer args: `[u64 count][(u64 name_len)(name)(tag)(payload)...]`
//! - lookup result envelope at `out_ptr`: `[status u32][len u32][u64 count]
//!   [row ids...]`, each id `u64`.
//! - `GET` result envelope: `[status u32][len u32][u8 present][u64 nvalues]
//!   [values...]`, each value `[u8 tag][payload]`. The `players` table is all
//!   U64/I64 columns, so every value is exactly 9 bytes; value *k*'s payload
//!   sits at `16402 + k*9`.
//!
//! The module never depends on host-call ordering across ops for
//! correctness: each result envelope is fully consumed before the next op
//! (which reuses the `out_ptr` envelope area). `$is_alive_row` performs a
//! candidate `GET` and the caller reads the still-intact envelope row
//! immediately after.

/// Builds the `fire_weapon` WASM module bytecode.
pub fn fire_weapon_module() -> Vec<u8> {
    wat::parse_str(WAT).expect("fire_weapon WAT is valid")
}

const WAT: &str = r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))

  ;; ---- string constants (offsets into linear memory) ----
  (data (i32.const 90000) "players")
  (data (i32.const 90100) "primary")
  (data (i32.const 90200) "pos")
  (data (i32.const 90300) "dead")
  (data (i32.const 90400) "recharging")
  (data (i32.const 90500) "out of ammo")
  (data (i32.const 90600) "not in arena")
  (data (i32.const 90700) "bad args")
  (data (i32.const 90800) "hit")
  (data (i32.const 90900) "kill")
  (data (i32.const 91000) "disconnected")
  (data (i32.const 91100) "schema mismatch")
  (data (i32.const 91200) "host op failed")

  ;; ---- helpers ----
  (func $mem_copy (param $dst i32) (param $src i32) (param $len i32)
    (local $k i32)
    (block $done
      (loop $loop
        (br_if $done (i32.ge_u (local.get $k) (local.get $len)))
        (i32.store8 align=1
          (i32.add (local.get $dst) (local.get $k))
          (i32.load8_u align=1 (i32.add (local.get $src) (local.get $k))))
        (local.set $k (i32.add (local.get $k) (i32.const 1)))
        (br $loop))))

  ;; put_str(dst, src, len) -> next offset
  (func $put_str (param $dst i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $dst) (i64.extend_i32_u (local.get $len)))
    (call $mem_copy (i32.add (local.get $dst) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $dst) (i32.add (i32.const 8) (local.get $len))))

  ;; put_u64(dst, v) -> next offset
  (func $put_u64 (param $dst i32) (param $v i64) (result i32)
    (i64.store align=1 (local.get $dst) (local.get $v))
    (i32.add (local.get $dst) (i32.const 8)))

  ;; put_value_u64(dst, v): tag 8 + payload -> next offset
  (func $put_value_u64 (param $dst i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $dst) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $dst) (i32.const 1)) (local.get $v))
    (i32.add (local.get $dst) (i32.const 9)))

  ;; put_value_i64(dst, v): tag 4 + payload -> next offset
  (func $put_value_i64 (param $dst i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $dst) (i32.const 4))
    (i64.store align=1 (i32.add (local.get $dst) (i32.const 1)) (local.get $v))
    (i32.add (local.get $dst) (i32.const 9)))

  ;; call_op(opcode, arg_len): args at 0, envelope at out_ptr
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))

  ;; return an I64 value from out_ptr
  (func $ret_i64 (param $v i64) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 4))
    (i64.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 9))

  ;; application rejection: [u32 len][msg] at out_ptr, return u32::MAX
  (func $reject (param $msg i32) (param $len i32) (result i32)
    (i32.store align=1 (i32.const 16384) (local.get $len))
    (call $mem_copy (i32.const 16388) (local.get $msg) (local.get $len))
    (i32.const -1))

  ;; is_alive_row(id) -> 1 if the row exists with the players schema and its
  ;; alive column is nonzero; 0 otherwise (including a host-op failure). The
  ;; GET envelope is left intact for the caller to read the row values.
  (func $is_alive_row (param $id i64) (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (local.get $id)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (if (i32.ne (i32.load align=1 (i32.const 16384)) (i32.const 0))
      (then (return (i32.const 0))))
    (if (i32.eq (i32.load8_u align=1 (i32.const 16392)) (i32.const 0))
      (then (return (i32.const 0))))
    (if (i64.ne (i64.load align=1 (i32.const 16393)) (i64.const 11))
      (then (return (i32.const 0))))
    (if (i64.eq (i64.load align=1 (i32.const 16447)) (i64.const 0))
      (then (return (i32.const 0))))
    (i32.const 1))

  (func (export "_nexum_reducer_run") (result i32)
    ;; ---- locals ----
    (local $p i32)          ;; cursor
    (local $caller i64)     ;; the shooter (stamped __caller)
    (local $self_rid i64)   ;; the shooter's row id (from the primary index)
    ;; self row (from GET)
    (local $s0 i64) (local $s1 i64) (local $s2 i64) (local $s3 i64) (local $s4 i64)
    (local $s5 i64) (local $s6 i64) (local $s7 i64) (local $s8 i64) (local $s9 i64) (local $s10 i64)
    ;; target row (from the position index + GET)
    (local $t_found i32)
    (local $t_rid i64)
    (local $t0 i64) (local $t1 i64) (local $t2 i64) (local $t3 i64) (local $t4 i64)
    (local $t5 i64) (local $t6 i64) (local $t7 i64) (local $t8 i64) (local $t9 i64) (local $t10 i64)
    (local $aimx i64) (local $aimy i64) (local $damage i64)
    (local $i i32)          ;; candidate cursor
    (local $n i32)          ;; candidate count
    (local $id i64)         ;; current candidate row id

    ;; ---- parse args: exactly one [u64 name_len][name][u8 tag][u64 payload] ----
    (local.set $p (i32.const 0))
    (if (i64.ne (i64.load align=1 (local.get $p)) (i64.const 1))
      (then (return (call $reject (i32.const 90700) (i32.const 8)))))
    ;; skip name: 8 (len) + len bytes
    (local.set $p (i32.add (local.get $p) (i32.const 8)))
    (local.set $p (i32.add (local.get $p)
      (i32.add (i32.wrap_i64 (i64.load align=1 (local.get $p))) (i32.const 8))))
    ;; skip tag
    (local.set $p (i32.add (local.get $p) (i32.const 1)))
    (local.set $caller (i64.load align=1 (local.get $p)))

    ;; ---- lookup_unique("players", "primary", [caller]) -> shooter row id ----
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_str (local.get $p) (i32.const 90100) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 1)))
    (local.set $p (call $put_value_u64 (local.get $p) (local.get $caller)))
    (drop (call $call_op (i32.const 4) (local.get $p)))
    (if (i32.ne (i32.load align=1 (i32.const 16384)) (i32.const 0))
      (then (return (call $reject (i32.const 91200) (i32.const 14)))))
    (if (i64.eq (i64.load align=1 (i32.const 16392)) (i64.const 0))
      (then (return (call $reject (i32.const 90600) (i32.const 11)))))
    (local.set $self_rid (i64.load align=1 (i32.const 16400)))

    ;; ---- get the shooter row ----
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (local.get $self_rid)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (if (i32.ne (i32.load align=1 (i32.const 16384)) (i32.const 0))
      (then (return (call $reject (i32.const 91200) (i32.const 14)))))
    (if (i32.eq (i32.load8_u align=1 (i32.const 16392)) (i32.const 0))
      (then (return (call $reject (i32.const 90600) (i32.const 11)))))
    (if (i64.ne (i64.load align=1 (i32.const 16393)) (i64.const 11))
      (then (return (call $reject (i32.const 91100) (i32.const 15)))))
    ;; values at 16401; value k payload at 16402 + k*9
    (local.set $s0 (i64.load align=1 (i32.const 16402)))
    (local.set $s1 (i64.load align=1 (i32.const 16411)))
    (local.set $s2 (i64.load align=1 (i32.const 16420)))
    (local.set $s3 (i64.load align=1 (i32.const 16429)))
    (local.set $s4 (i64.load align=1 (i32.const 16438)))
    (local.set $s5 (i64.load align=1 (i32.const 16447)))
    (local.set $s6 (i64.load align=1 (i32.const 16456)))
    (local.set $s7 (i64.load align=1 (i32.const 16465)))
    (local.set $s8 (i64.load align=1 (i32.const 16474)))
    (local.set $s9 (i64.load align=1 (i32.const 16483)))
    (local.set $s10 (i64.load align=1 (i32.const 16492)))

    ;; ---- validate the shooter ----
    (if (i64.eq (local.get $s5) (i64.const 0))
      (then (return (call $reject (i32.const 90300) (i32.const 4)))))
    (if (i64.eq (local.get $s10) (i64.const 0))
      (then (return (call $reject (i32.const 91000) (i32.const 12)))))
    (if (i64.gt_s (local.get $s7) (i64.const 0))
      (then (return (call $reject (i32.const 90400) (i32.const 10)))))
    (if (i64.le_s (local.get $s9) (i64.const 0))
      (then (return (call $reject (i32.const 90500) (i32.const 12)))))

    ;; ---- aim cell from the authoritative facing (0 N, 1 E, 2 S, 3 W) ----
    (local.set $aimx (local.get $s1))
    (local.set $aimy (local.get $s2))
    (if (i64.eq (local.get $s8) (i64.const 0))
      (then (local.set $aimy (i64.sub (local.get $s2) (i64.const 1)))))
    (if (i64.eq (local.get $s8) (i64.const 1))
      (then (local.set $aimx (i64.add (local.get $s1) (i64.const 1)))))
    (if (i64.eq (local.get $s8) (i64.const 2))
      (then (local.set $aimy (i64.add (local.get $s2) (i64.const 1)))))
    (if (i64.eq (local.get $s8) (i64.const 3))
      (then (local.set $aimx (i64.sub (local.get $s1) (i64.const 1)))))

    ;; ---- lookup_index("players", "pos", [aimx, aimy]) -> candidate ids ----
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_str (local.get $p) (i32.const 90200) (i32.const 3)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 2)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $aimx)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $aimy)))
    (drop (call $call_op (i32.const 9) (local.get $p)))
    (if (i32.ne (i32.load align=1 (i32.const 16384)) (i32.const 0))
      (then (return (call $reject (i32.const 91200) (i32.const 14)))))
    (local.set $n (i32.wrap_i64 (i64.load align=1 (i32.const 16392))))
    (local.set $i (i32.const 0))

    ;; ---- candidate loop: pick the first alive non-self row at the cell ----
    (block $cand_done
      (loop $cand
        (br_if $cand_done (i32.ge_u (local.get $i) (local.get $n)))
        (local.set $id
          (i64.load align=1 (i32.add (i32.const 16400) (i32.mul (local.get $i) (i32.const 8)))))
        (if (i32.eqz (local.get $t_found))
          (then
            (if (i64.ne (local.get $id) (local.get $self_rid))
              (then
                (if (call $is_alive_row (local.get $id))
                  (then
                    (local.set $t_found (i32.const 1))
                    (local.set $t_rid (local.get $id))
                    (local.set $t0 (i64.load align=1 (i32.const 16402)))
                    (local.set $t1 (i64.load align=1 (i32.const 16411)))
                    (local.set $t2 (i64.load align=1 (i32.const 16420)))
                    (local.set $t3 (i64.load align=1 (i32.const 16429)))
                    (local.set $t4 (i64.load align=1 (i32.const 16438)))
                    (local.set $t5 (i64.load align=1 (i32.const 16447)))
                    (local.set $t6 (i64.load align=1 (i32.const 16456)))
                    (local.set $t7 (i64.load align=1 (i32.const 16465)))
                    (local.set $t8 (i64.load align=1 (i32.const 16474)))
                    (local.set $t9 (i64.load align=1 (i32.const 16483)))
                    (local.set $t10 (i64.load align=1 (i32.const 16492)))))))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cand)))

    ;; ---- consume the shot: update the shooter row (cooldown, ammo) ----
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (local.get $self_rid)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 11)))
    (local.set $p (call $put_value_u64 (local.get $p) (local.get $s0)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s1)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s2)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s3)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s4)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s5)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s6)))
    (local.set $p (call $put_value_i64 (local.get $p) (i64.const 5)))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s8)))
    (local.set $p (call $put_value_i64 (local.get $p) (i64.sub (local.get $s9) (i64.const 1))))
    (local.set $p (call $put_value_i64 (local.get $p) (local.get $s10)))
    (drop (call $call_op (i32.const 6) (local.get $p)))

    ;; ---- resolve the hit ----
    (local.set $damage (i64.const 0))
    (if (local.get $t_found)
      (then
        (local.set $damage (i64.const 25))
        (local.set $t3 (i64.sub (local.get $t3) (i64.const 25)))
        (if (i64.lt_s (local.get $t3) (i64.const 0))
          (then (local.set $t3 (i64.const 0))))
        (if (i64.eq (local.get $t3) (i64.const 0))
          (then (local.set $t5 (i64.const 0))))
        ;; update the target row (hp, alive)
        (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
        (local.set $p (call $put_u64 (local.get $p) (local.get $t_rid)))
        (local.set $p (call $put_u64 (local.get $p) (i64.const 11)))
        (local.set $p (call $put_value_u64 (local.get $p) (local.get $t0)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t1)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t2)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t3)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t4)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t5)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t6)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t7)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t8)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t9)))
        (local.set $p (call $put_value_i64 (local.get $p) (local.get $t10)))
        (drop (call $call_op (i32.const 6) (local.get $p)))
        ;; emit "hit" (shooter)
        (local.set $p (call $put_str (i32.const 0) (i32.const 90800) (i32.const 3)))
        (local.set $p (call $put_value_u64 (local.get $p) (local.get $caller)))
        (drop (call $call_op (i32.const 8) (local.get $p)))
        ;; emit "kill" (target) when the target died
        (if (i64.eq (local.get $t3) (i64.const 0))
          (then
            (local.set $p (call $put_str (i32.const 0) (i32.const 90900) (i32.const 4)))
            (local.set $p (call $put_value_u64 (local.get $p) (local.get $t0)))
            (drop (call $call_op (i32.const 8) (local.get $p)))))))
    (return (call $ret_i64 (local.get $damage))))
)
"#;
