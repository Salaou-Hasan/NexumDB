//! A single subscription: its derived view, cursor, buffer, and lifecycle.
//!
//! The **view** is a derived cache over authoritative state (ADR-008 D5):
//!
//! - `window` — **all** rows matching the query's predicates, sorted by the
//!   query's sort key (or by `RowId` when there is none). Each entry carries
//!   the row payload so that window backfill can produce `Insert` deltas
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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

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

/// One subscription: query, compiled matcher, derived view, cursor, buffer.
#[derive(Debug)]
pub struct Subscription {
    id: crate::SubscriptionId,
    query: Query,
    compiled: CompiledQuery,
    state: SubscriptionState,
    /// The commit sequence at the current observation point (ADR-008 D3).
    cursor: u64,
    /// The effective window cap: the query's `limit` (if any), never larger
    /// than the registry's `max_snapshot_rows` safety bound.
    window_cap: usize,
    /// Every matching row, sorted by the query ordering.
    window: BTreeMap<Key, Row>,
    /// The delivered membership: the top-`window_cap` rows of `window`.
    visible_ids: BTreeSet<RowId>,
    buffer: Vec<SubscriptionUpdate>,
    max_buffered: usize,
}

impl Subscription {
    /// Creates an empty, `Active` subscription at cursor `0`.
    pub(crate) fn new(
        id: crate::SubscriptionId,
        query: Query,
        compiled: CompiledQuery,
        max_buffered: usize,
        max_snapshot_rows: usize,
    ) -> Self {
        let window_cap = compiled.limit().unwrap_or(usize::MAX).min(max_snapshot_rows);
        Self {
            id,
            query,
            compiled,
            state: SubscriptionState::Active,
            cursor: 0,
            window_cap,
            window: BTreeMap::new(),
            visible_ids: BTreeSet::new(),
            buffer: Vec::new(),
            max_buffered,
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

    /// Returns the number of rows currently in the delivered view.
    pub fn visible_len(&self) -> usize {
        self.visible_ids.len()
    }

    /// Returns the observed table id (pinned at compile time).
    pub(crate) fn table_id(&self) -> TableId {
        self.compiled.table_id()
    }

    /// Replaces the compiled matcher (used by `resync` to re-resolve the
    /// query by name — e.g. after a table drop + recreate).
    pub(crate) fn set_compiled(&mut self, compiled: CompiledQuery) {
        self.compiled = compiled;
    }

    /// Establishes the view from a full authoritative scan: filters, orders,
    /// applies the window cap, projects, and records the derived state.
    /// Returns the delivered rows in query order.
    pub(crate) fn rebuild(&mut self, rows: Vec<(RowId, Row)>) -> Vec<DeliveredRow> {
        self.window.clear();
        self.visible_ids.clear();
        let mut ordered: Vec<(Key, Row)> = rows
            .into_iter()
            .map(|(row_id, row)| (self.key_of(&row, row_id), row))
            .collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, row) in &ordered {
            self.window.insert(key.clone(), row.clone());
        }
        for key in self.window.keys().take(self.window_cap) {
            self.visible_ids.insert(key.row_id);
        }
        ordered.truncate(self.window_cap);
        ordered
            .into_iter()
            .map(|(key, row)| DeliveredRow::new(key.row_id, self.compiled.project(&row)))
            .collect()
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

    /// Applies one committed change to the derived view, appending deltas to
    /// the buffer. May mark the subscription stale on buffer overflow.
    pub(crate) fn apply_change(&mut self, change: &Change, seq: u64) {
        let row_id = change.row_id();
        match change.kind() {
            ChangeKind::Insert => {
                if let Some(new_row) = change.new_row()
                    && self.compiled.matches(new_row)
                {
                    // A brand-new row: it was never in the view before, so no
                    // `Update` is possible.
                    self.upsert(row_id, new_row, seq, false);
                }
            }
            ChangeKind::Update => {
                let (Some(_old_row), Some(new_row)) = (change.old_row(), change.new_row())
                else {
                    return;
                };
                let was_visible = self.is_visible(row_id);
                if self.compiled.matches(new_row) {
                    self.upsert(row_id, new_row, seq, was_visible);
                } else if self.find_key(row_id).is_some() {
                    self.remove(row_id);
                    self.sync_window(seq, None);
                }
            }
            ChangeKind::Delete => {
                if self.find_key(row_id).is_some() {
                    self.remove(row_id);
                    self.sync_window(seq, None);
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

    /// Locates the window entry for `row_id`, if any.
    fn find_key(&self, row_id: RowId) -> Option<Key> {
        self.window
            .iter()
            .find(|(key, _)| key.row_id == row_id)
            .map(|(key, _)| key.clone())
    }

    /// Removes `row_id` from the window, returning whether it was present.
    fn remove(&mut self, row_id: RowId) -> bool {
        if let Some(key) = self.find_key(row_id) {
            self.window.remove(&key);
            true
        } else {
            false
        }
    }

    /// Inserts or replaces `row_id`'s window entry (it now matches) and
    /// re-synchronizes the window. `was_visible` controls the `Update` delta:
    /// when the row was visible before the change, an `Update` is emitted if
    /// it remains visible.
    fn upsert(&mut self, row_id: RowId, row: &Row, seq: u64, was_visible: bool) {
        self.remove(row_id);
        let key = self.key_of(row, row_id);
        self.window.insert(key, row.clone());
        let update = was_visible.then(|| DeliveredRow::new(row_id, self.compiled.project(row)));
        self.sync_window(seq, update);
    }

    /// Re-synchronizes the delivered view to the exact top-`window_cap`
    /// rows of `window` and emits the membership changes. Emission order per
    /// commit: the changed row's `Update` first (when it stays visible),
    /// then `Delete`s for rows that left the window, then `Insert`s for rows
    /// that entered — each group in ascending `RowId` order.
    fn sync_window(&mut self, seq: u64, update: Option<DeliveredRow>) {
        let mut new_visible = BTreeSet::new();
        for key in self.window.keys().take(self.window_cap) {
            new_visible.insert(key.row_id);
        }
        let left: Vec<RowId> = self
            .visible_ids
            .difference(&new_visible)
            .copied()
            .collect();
        let entered: Vec<RowId> = new_visible
            .difference(&self.visible_ids)
            .copied()
            .collect();
        self.visible_ids = new_visible;

        if let Some(update) = update.filter(|update| self.is_visible(update.row_id())) {
            self.push(SubscriptionUpdate::Update { seq, row: update }, seq);
        }
        for row_id in left {
            self.push(SubscriptionUpdate::Delete { seq, row_id }, seq);
        }
        for row_id in entered {
            let row = self
                .window
                .iter()
                .find(|(key, _)| key.row_id == row_id)
                .map(|(_, row)| row.clone())
                .expect("entered rows come from the window");
            self.push(
                SubscriptionUpdate::Insert {
                    seq,
                    row: DeliveredRow::new(row_id, self.compiled.project(&row)),
                },
                seq,
            );
        }
    }

    /// Buffers one update, marking the subscription stale on overflow.
    /// Once stale, further deltas of the same commit are dropped entirely
    /// (the `Stale` marker is the only signal; `resync` rebuilds the view).
    fn push(&mut self, update: SubscriptionUpdate, seq: u64) {
        if self.state == SubscriptionState::Stale {
            return;
        }
        self.buffer.push(update);
        if self.buffer.len() > self.max_buffered {
            self.mark_stale(seq);
        }
    }
}
