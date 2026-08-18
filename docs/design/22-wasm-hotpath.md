# Phase 22 — Transaction Overlay Optimization

## Problem

The gameplay hot-path under burst load exhibited O(N²) behavior due to:

1. **O(N) branch cost**: `branch()` cloned the parent's entire write set
2. **O(N) lookup scan**: `lookup_unique`/`lookup_index` scanned all write entries
3. **Clone overhead**: absorb cloned WriteEntry values unnecessarily

## Solution: COW WriteSet

### Data Structure Change

```
Before:
  struct WriteSet {
      base: Option<Arc<Map>>,
      own: Map,           // BTreeMap<(TableId, RowId), WriteEntry>
  }

After:
  struct WriteSet {
      base: Option<Arc<Map>>,
      own: Arc<Map>,      // Arc-wrapped for O(1) branch
  }
```

### Branch: O(1)

```
Before: Arc::new(self.own.clone())  // O(N) deep copy
After:  Arc::clone(&self.own)       // O(1) refcount bump
```

### Write Operations: Arc::make_mut

All write operations (`insert`, `update`, `delete`, `set`, `remove`) go through
`own_mut()` which calls `Arc::make_mut(&mut self.own)`. This is O(1) when the
Arc has refcount == 1 (the common case for the transaction owner).

### Absorb: Destructure + Move

`absorb` takes the child by value. It destructures the child to extract `own`
entries and drop `base` (which may reference the parent's Arc). This brings the
parent's Arc refcount to 1, making `make_mut` O(1).

Fast path: when no Delete entries exist (the common case for update-heavy
workloads), absorb skips the logical-view check entirely and moves entries
via `Arc::try_unwrap`.

## Solution: has_any_insert() Skip

`lookup_unique` and `lookup_index` previously scanned all write entries to find
pending inserts matching the index key. The new `has_any_insert()` method
returns false when no Insert entries exist in the logical view, allowing the
scan to be skipped entirely.

This is correct because:
- Pending Updates are handled by the first loop (checking committed index entries)
- Pending Inserts are the only entries that can newly own an index key
- If there are no Inserts, the second scan would find nothing

## Correctness

- The logical view (own + base) is preserved exactly
- Insert/Delete coalescing rules are unchanged
- Cross-branch net-no-op resolution is preserved
- The absorb fold applies the same coalescing rules as before
- All 96 transaction tests pass
- All 19 parallel execution tests pass

## Tradeoffs

- `own` is now behind an Arc, adding one level of indirection for reads
  (negligible cost, same as base layer)
- Write operations go through `Arc::make_mut` which checks refcount
  (negligible when refcount == 1)
- Absorb's slow path (with Delete entries) still clones via `.cloned()`
  (only triggered when child has Delete entries)
