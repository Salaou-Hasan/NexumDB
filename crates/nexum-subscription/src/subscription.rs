//! A single subscription: its per-member delivery state, and the **shared
//! derived view** it belongs to (ADR-020 D1).
//!
//! The **view** is a derived cache over authoritative state (ADR-008 D5):
//!
//! - `window` — **all** rows matching the query's predicates, sorted by the
//!   query's sort key (or by `RowId` when there is none). Each entry carries
//!   the row payload (an `Arc<Row>` shared across identical subscriptions,
//!   ADR-019 D4) so that window backfill can produce `Insert` deltas
//!   without touching storage;
//! - `visible_ids` — the *delivered* membership: the top-`window_cap` rows
//!   of `window` (the query's `limit`, capped by the registry's
//!   `max_snapshot_rows` bound).
//!
//! Every committed change updates `window` and then re-synchronizes
//! `visible_ids` to the exact top-N — so the delivered view always equals
//! the authoritative top-N at every committed point (design notes §8),
//! including rows entering or leaving the window boundary. Both structures
//! are facets of the same derived set, rebuilt from a full scan on
//! establishment and resync, and neither feeds back into authoritative
//! state. The delta stream is emitted in deterministic order per commit:
//! the changed row's `Update` first (when it stays visible), then `Delete`s
//! for rows that left the window (ascending `RowId`), then `Insert`s for
//! rows that entered (ascending `RowId`).
//!
//! Identical queries produce identical views and identical delta streams,
//! so the registry shares **one** view per distinct query and evaluates
//! each committed change once per group, then fans the resulting deltas
//! out to every member's buffer (ADR-020 D1). This turns the measured
//! O(changes × subscriptions) fan-out into O(changes × distinct_queries)
//! evaluation plus a window-sized per-member clone — the Phase 19 finding
//! that the number of evaluations, not their unit cost, was the
//! bottleneck.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

use nexum_core::{ChangeKind, Row, RowId, TableId, Value};
use nexum_storage::Change;

use crate::delta::{DeliveredRow, SubscriptionUpdate};
use crate::matcher::{value_cmp, CompiledQuery};
use crate::query::Query;

/// Lifecycle state of a subscription (ADR-008 D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// The view is current; deltas are being delivered.
    Active,
    /// The subscription fell behind (buffer overflow or its table was
    /// dropped). Deltas are dropped until a `resync` rebuilds the view.
    Stale,
}

/// Deterministic sort key of the visible window: the query's sort value
/// (absent when the query has no `order_by`), with `RowId` as the tie-break.
/// Descending order flips only the sort-value comparison, so equal sort
/// values still order by ascending `RowId`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Key {
    sort: Option<Value>,
    row_id: RowId,
    descending: bool,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        let primary = match (&self.sort, &other.sort) {
            (Some(a), Some(b)) => value_cmp(a, b),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        let primary = if self.descending {
            primary.reverse()
        } else {
            primary
        };
        primary.then_with(|| self.row_id.cmp(&other.row_id))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The **shared derived view** of one distinct query: the window plus its
/// membership mirrors. Owned by the registry (ADR-020 D1) and referenced
/// by every member subscription with an identical query. Evaluated once
/// per committed change per group; the registry fans the produced deltas
/// out to each member's buffer.
#[derive(Debug)]
pub(crate) struct SharedView {
    query: Query,
    compiled: CompiledQuery,
    /// The effective window cap: the query's `limit` (if any), never larger
    /// than the registry's `max_snapshot_rows` safety bound.
    window_cap: usize,
    /// Every matching row, sorted by the query ordering. Payloads are
    /// `Arc<Row>` **shared across subscriptions** (ADR-019 D4): the commit
    /// path wraps each committed row once, so per-subscription window
    /// maintenance bumps a refcount instead of deep-cloning the row.
    window: BTreeMap<Key, Arc<Row>>,
    /// `row_id → key` mirror of `window` (ADR-015 D5): turns the per-change
    /// membership lookups from O(window) scans into O(log N) lookups while
    /// keeping the authoritative window structure unchanged.
    row_keys: BTreeMap<RowId, Key>,
    /// The exact top-`window_cap` keys of `window` — the delivered
    /// membership maintained incrementally across commits (ADR-015 D5), so a
    /// single-row change never rebuilds the whole cap.
    visible_keys: BTreeSet<Key>,
    /// The delivered membership: the top-`window_cap` rows of `window`.
    visible_ids: BTreeSet<RowId>,
}

impl SharedView {
    /// Creates an empty view for `query`.
    pub(crate) fn new(query: Query, compiled: CompiledQuery, max_snapshot_rows: usize) -> Self {
        let window_cap = compiled.limit().unwrap_or(usize::MAX).min(max_snapshot_rows);
        Self {
            query,
            compiled,
            window_cap,
            window: BTreeMap::new(),
            row_keys: BTreeMap::new(),
            visible_keys: BTreeSet::new(),
            visible_ids: BTreeSet::new(),
        }
    }

    /// Returns the view's logical query (the dedup identity).
    pub(crate) fn query(&self) -> &Query {
        &self.query
    }

    /// Returns the observed table id (pinned at compile time).
    pub(crate) fn table_id(&self) -> TableId {
        self.compiled.table_id()
    }

    /// Returns the number of rows currently in the delivered view.
    pub(crate) fn visible_len(&self) -> usize {
        self.visible_ids.len()
    }

    /// Replaces the compiled matcher (used by `resync` to re-resolve the
    /// query by name — e.g. after a table drop + recreate).
    pub(crate) fn set_compiled(&mut self, compiled: CompiledQuery) {
        self.compiled = compiled;
    }

    /// Establishes the view from a full authoritative scan: filters, orders,
    /// applies the window cap, projects, and records the derived state.
    /// Returns the delivered rows in query order.
    ///
    /// Each scanned row is moved into the window exactly once (no second
    /// clone — ADR-015 D5); the `row_keys` mirror is built in the same pass.
    pub(crate) fn rebuild(&mut self, rows: Vec<(RowId, Row)>) -> Vec<DeliveredRow> {
        self.window.clear();
        self.row_keys.clear();
        self.visible_keys.clear();
        self.visible_ids.clear();
        let mut ordered: Vec<(Key, Row)> = rows
            .into_iter()
            .map(|(row_id, row)| (self.key_of(&row, row_id), row))
            .collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        let mut delivered = Vec::with_capacity(self.window_cap.min(ordered.len()));
        for (key, row) in ordered {
            self.row_keys.insert(key.row_id, key.clone());
            self.window.insert(key, Arc::new(row));
        }
        for key in self.window.keys().take(self.window_cap) {
            self.visible_ids.insert(key.row_id);
            self.visible_keys.insert(key.clone());
            let row = self.window.get(key).expect("key iterated from the window");
            delivered.push(DeliveredRow::new(key.row_id, self.compiled.project(row)));
        }
        delivered
    }

    /// The current delivered rows in query order — the Initial snapshot for
    /// a member joining a live group, whose view is already current.
    pub(crate) fn visible_rows(&self) -> Vec<DeliveredRow> {
        self.visible_keys
            .iter()
            .map(|key| {
                let row = self
                    .window
                    .get(key)
                    .expect("visible keys come from the window");
                DeliveredRow::new(key.row_id, self.compiled.project(row))
            })
            .collect()
    }

    /// Applies one committed change to the derived view, appending the
    /// resulting updates to `out` (the group's scratch buffer). The caller
    /// fans `out` out to each member's delivery buffer; overflow/stale
    /// handling is per member (ADR-020 D1).
    pub(crate) fn apply_change(&mut self, change: &Change, seq: u64, out: &mut Vec<SubscriptionUpdate>) {
        let row_id = change.row_id();
        match change.kind() {
            ChangeKind::Insert => {
                if let Some(new_row) = change.new_row_shared()
                    && self.compiled.matches(new_row)
                {
                    // A brand-new row: it was never in the view before, so no
                    // `Update` is possible.
                    self.upsert(row_id, new_row, seq, out, false);
                }
            }
            ChangeKind::Update => {
                let (Some(_old_row), Some(new_row)) =
                    (change.old_row(), change.new_row_shared())
                else {
                    return;
                };
                let was_visible = self.is_visible(row_id);
                if self.compiled.matches(new_row) {
                    self.upsert(row_id, new_row, seq, out, was_visible);
                } else if let Some(old) = self.remove(row_id) {
                    self.sync_window(seq, out, None, Some(old), None);
                }
            }
            ChangeKind::Delete => {
                if let Some(old) = self.remove(row_id) {
                    self.sync_window(seq, out, None, Some(old), None);
                }
            }
        }
    }

    /// Returns `true` if `row_id` is in the delivered view.
    fn is_visible(&self, row_id: RowId) -> bool {
        self.visible_ids.contains(&row_id)
    }

    /// Builds the sort key for a row.
    fn key_of(&self, row: &Row, row_id: RowId) -> Key {
        Key {
            sort: self.compiled.sort_value(row),
            row_id,
            descending: self.compiled.descending(),
        }
    }

    /// Removes `row_id` from the window, returning its key if it was
    /// present. O(log N) via the `row_keys` mirror (ADR-015 D5).
    fn remove(&mut self, row_id: RowId) -> Option<Key> {
        let key = self.row_keys.remove(&row_id)?;
        self.window.remove(&key);
        Some(key)
    }

    /// Inserts or replaces `row_id`'s window entry (it now matches) and
    /// re-synchronizes the window. `was_visible` controls the `Update` delta:
    /// when the row was visible before the change, an `Update` is emitted if
    /// it remains visible. The row is retained as a shared `Arc` (ADR-019
    /// D4) — a refcount bump per group rather than a deep clone.
    fn upsert(
        &mut self,
        row_id: RowId,
        row: &Arc<Row>,
        seq: u64,
        out: &mut Vec<SubscriptionUpdate>,
        was_visible: bool,
    ) {
        let old = self.remove(row_id);
        let key = self.key_of(row, row_id);
        self.row_keys.insert(row_id, key.clone());
        self.window.insert(key.clone(), Arc::clone(row));
        let update = was_visible.then(|| DeliveredRow::new(row_id, self.compiled.project(row)));
        self.sync_window(seq, out, update, old, Some(key));
    }

    /// Re-synchronizes the delivered view to the exact top-`window_cap`
    /// rows of `window` and appends the membership changes to `out`.
    /// Emission order per commit: the changed row's `Update` first (when it
    /// stays visible), then `Delete`s for rows that left the window, then
    /// `Insert`s for rows that entered.
    ///
    /// Incremental (ADR-015 D5): one changed row's key can move across the
    /// window boundary at most once, so the visible set is adjusted locally
    /// — O(log N) plus the number of emitted deltas — instead of rebuilding
    /// the top-`window_cap` on every commit. `ko` is the row's key before
    /// the change (None if it was not in the window); `kn` is its key after
    /// (None for a removal). The caller already updated `window`/`row_keys`.
    fn sync_window(
        &mut self,
        seq: u64,
        out: &mut Vec<SubscriptionUpdate>,
        update: Option<DeliveredRow>,
        ko: Option<Key>,
        kn: Option<Key>,
    ) {
        let was_visible = ko
            .as_ref()
            .is_some_and(|old| self.visible_keys.remove(old));
        let Some(kn) = kn else {
            // Removal path: the row is gone from the window.
            if was_visible {
                let row_id = ko.expect("was_visible implies an old key").row_id;
                self.visible_ids.remove(&row_id);
                out.push(SubscriptionUpdate::Delete { seq, row_id });
                self.backfill(seq, out);
            }
            #[cfg(debug_assertions)]
            self.debug_check_invariants();
            return;
        };
        let row_id = kn.row_id;
        if was_visible {
            // The row was in the top-cap. It remains visible iff its new key
            // is still within the cap: it ranks at or above the smallest key
            // the remaining visible rows leave open.
            let still_visible = match self.visible_keys.last() {
                None => true, // cap == 1 and it was the only visible row
                Some(max) => self
                    .window
                    .range((Bound::Excluded(max.clone()), Bound::Unbounded))
                    .next()
                    .is_none_or(|(next, _)| &kn <= next),
            };
            // No key above the remaining visible set means `kn` ranks within
            // it (or the window is under capacity), so the row stays visible.
            if still_visible {
                self.visible_keys.insert(kn);
                if let Some(upd) = update {
                    out.push(SubscriptionUpdate::Update { seq, row: upd });
                }
            } else {
                // Demoted: Delete, then the next-best row backfills.
                self.visible_ids.remove(&row_id);
                out.push(SubscriptionUpdate::Delete { seq, row_id });
                self.backfill(seq, out);
            }
        } else {
            // The row was not visible. It enters iff it ranks within the cap
            // (or the window is under capacity).
            let enters = self.visible_keys.len() < self.window_cap
                || self.visible_keys.last().is_none_or(|worst| &kn <= worst);
            if enters {
                self.visible_keys.insert(kn.clone());
                self.visible_ids.insert(row_id);
                let evicted = if self.visible_keys.len() > self.window_cap {
                    self.visible_keys.pop_last()
                } else {
                    None
                };
                // Delete the evicted row first, then Insert the new row
                // (matches the historical per-commit order).
                if let Some(evicted) = &evicted {
                    self.visible_ids.remove(&evicted.row_id);
                    out.push(SubscriptionUpdate::Delete {
                        seq,
                        row_id: evicted.row_id,
                    });
                }
                let row = self.deliverable(row_id);
                out.push(SubscriptionUpdate::Insert {
                    seq,
                    row: DeliveredRow::new(row_id, self.compiled.project(&row)),
                });
            }
            // else: stays invisible; nothing changes.
        }
        #[cfg(debug_assertions)]
        self.debug_check_invariants();
    }

    /// Promotes the next-best window row into the visible set after a row
    /// left it. No-op when the window is under capacity (nothing to promote).
    fn backfill(&mut self, seq: u64, out: &mut Vec<SubscriptionUpdate>) {
        let Some(entered) = self.next_after_visible() else {
            return;
        };
        self.visible_ids.insert(entered.row_id);
        self.visible_keys.insert(entered.clone());
        let row = self.deliverable(entered.row_id);
        out.push(SubscriptionUpdate::Insert {
            seq,
            row: DeliveredRow::new(entered.row_id, self.compiled.project(&row)),
        });
    }

    /// The smallest window key above the current visible set: the next row
    /// to promote when the window is at capacity, or the smallest window key
    /// when the visible set is empty.
    fn next_after_visible(&self) -> Option<Key> {
        match self.visible_keys.last() {
            Some(max) => self
                .window
                .range((Bound::Excluded(max.clone()), Bound::Unbounded))
                .next()
                .map(|(key, _)| key.clone()),
            None => self.window.iter().next().map(|(key, _)| key.clone()),
        }
    }

    /// The current window row for `row_id` (cloned for delivery).
    fn deliverable(&self, row_id: RowId) -> Row {
        let key = self
            .row_keys
            .get(&row_id)
            .expect("delivered rows come from the window");
        self.window
            .get(key)
            .expect("row_keys mirrors the window")
            .as_ref()
            .clone()
    }

    /// The shared row payload currently in the window for `row_id`, if
    /// present — the exact `Arc` installed by the last change or rebuild.
    /// Test-only introspection proving payload sharing (ADR-019 D4).
    #[cfg(test)]
    pub(crate) fn window_row(&self, row_id: RowId) -> Option<&Arc<Row>> {
        let key = self.row_keys.get(&row_id)?;
        self.window.get(key)
    }

    /// Debug-only: the incremental visible set must equal the exact top-cap
    /// of the window at every committed point (the Phase 8 contract).
    #[cfg(debug_assertions)]
    fn debug_check_invariants(&self) {
        let mut expected_ids = BTreeSet::new();
        let mut expected_keys = BTreeSet::new();
        for key in self.window.keys().take(self.window_cap) {
            expected_ids.insert(key.row_id);
            expected_keys.insert(key.clone());
        }
        debug_assert_eq!(
            self.visible_ids, expected_ids,
            "visible_ids must be the exact top-cap"
        );
        debug_assert_eq!(
            self.visible_keys, expected_keys,
            "visible_keys must be the exact top-cap"
        );
    }
}

/// One member of a query group (ADR-020 D1): its own delivery state — id,
/// lifecycle, cursor, and bounded buffer — plus the index of the shared
/// view it observes. Members with identical queries share one view; each
/// member's buffer stays independent.
#[derive(Debug)]
pub struct Subscription {
    id: crate::SubscriptionId,
    query: Query,
    state: SubscriptionState,
    /// The commit sequence at the current observation point (ADR-008 D3).
    cursor: u64,
    buffer: Vec<SubscriptionUpdate>,
    max_buffered: usize,
    /// Index of this member's shared view in the registry (ADR-020 D1).
    view: usize,
}

impl Subscription {
    /// Creates an empty, `Active` member at cursor `0`, observing the shared
    /// view at index `view`.
    pub(crate) fn new(
        id: crate::SubscriptionId,
        query: Query,
        max_buffered: usize,
        view: usize,
    ) -> Self {
        Self {
            id,
            query,
            state: SubscriptionState::Active,
            cursor: 0,
            buffer: Vec::new(),
            max_buffered,
            view,
        }
    }

    /// Returns the subscription id.
    pub fn id(&self) -> crate::SubscriptionId {
        self.id
    }

    /// Returns the logical query (the serializable subscription definition).
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Returns the lifecycle state.
    pub fn state(&self) -> SubscriptionState {
        self.state
    }

    /// Returns the observation cursor: the commit sequence at the current
    /// observation point.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Returns the number of pending, undelivered updates.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the index of this member's shared view (ADR-020 D1).
    pub(crate) fn view(&self) -> usize {
        self.view
    }

    /// Installs the initial snapshot: records the cursor and delivers the
    /// `Initial` update.
    pub(crate) fn receive_initial(&mut self, seq: u64, rows: Vec<DeliveredRow>) {
        self.cursor = seq;
        self.state = SubscriptionState::Active;
        self.buffer.clear();
        self.buffer.push(SubscriptionUpdate::Initial { seq, rows });
    }

    /// Installs a resynced view: rebuilds the buffer (cleared first) and
    /// returns to `Active`.
    pub(crate) fn receive_resync(&mut self, seq: u64, rows: Vec<DeliveredRow>) {
        self.cursor = seq;
        self.state = SubscriptionState::Active;
        self.buffer.clear();
        self.buffer.push(SubscriptionUpdate::Resync { seq, rows });
    }

    /// Marks the subscription stale: clears the buffer, emits a single
    /// `Stale` update, and drops further deltas until a resync.
    pub(crate) fn mark_stale(&mut self, seq: u64) {
        self.buffer.clear();
        self.buffer.push(SubscriptionUpdate::Stale { seq });
        self.state = SubscriptionState::Stale;
    }

    /// Takes the pending updates, leaving the buffer empty.
    pub(crate) fn take_buffer(&mut self) -> Vec<SubscriptionUpdate> {
        std::mem::take(&mut self.buffer)
    }

    /// Appends one commit's delta stream — produced once by the shared view
    /// (ADR-020 D1) — to this member's buffer, replicating the historical
    /// per-subscription overflow/stale behavior exactly: deltas are appended
    /// in order until the buffer overflows, at which point the buffer is
    /// cleared, a single `Stale` update is emitted, and the rest of the
    /// commit is dropped.
    pub(crate) fn push_commit(&mut self, deltas: &[SubscriptionUpdate], seq: u64) {
        if self.state == SubscriptionState::Stale {
            return;
        }
        // Fast path: the whole commit fits.
        if self.buffer.len().saturating_add(deltas.len()) <= self.max_buffered {
            self.buffer.extend_from_slice(deltas);
            return;
        }
        for delta in deltas {
            if self.state == SubscriptionState::Stale {
                return;
            }
            self.buffer.push(delta.clone());
            if self.buffer.len() > self.max_buffered {
                self.mark_stale(seq);
            }
        }
    }
}
