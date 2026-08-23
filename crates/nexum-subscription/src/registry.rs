//! The subscription registry (ADR-008, ADR-020 D1).
//!
//! The registry owns all subscriptions and the **commit sequence** — the
//! monotonic observation cursor. Every `apply_changes` call represents one
//! committed transaction at the `Vec<Change>` boundary and consumes one
//! sequence number in commit order:
//!
//! ```text
//! Transaction::commit() → Vec<Change> → apply_changes(store, &changes)
//!                                            │
//!                                            ├─ WAL (the caller)
//!                                            └─ per-subscription deltas
//! ```
//!
//! **Interest management (ADR-020 D1):** subscriptions with **identical
//! queries** share one derived view, which is evaluated **once per distinct
//! query per commit**; the resulting delta stream is then fanned out to
//! each member's independent buffer. This turns the measured O(changes ×
//! subscriptions) fan-out into O(changes × distinct_queries) evaluation
//! plus a window-sized per-member clone. Identical queries produce
//! identical views and delta streams, so the grouped path is value-identical
//! to the historical per-subscription path (proven by the unchanged suite).
//!
//! Establishment is atomic (ADR-008 D4): `subscribe` captures the cursor
//! **before** scanning authoritative state, inside exclusive registry
//! ownership, so no committed change can fall between the snapshot and the
//! live stream.

use std::collections::{BTreeMap, BTreeSet};

use nexum_core::{Error, Result, SubscriptionId, TableId};
use nexum_storage::Change;
use nexum_table::TableStore;

use crate::config::SubscriptionConfig;
use crate::delta::SubscriptionUpdate;
use crate::matcher::{compile, matching_rows};
use crate::query::Query;
use crate::subscription::{SharedView, Subscription, SubscriptionState};

/// The result of one `apply_changes` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    seq: u64,
    affected: Vec<SubscriptionId>,
    /// `apply_change` evaluations performed: one per (change × distinct
    /// query) on the shared views (ADR-020 D3).
    evaluations: u64,
    /// Subscription updates produced by the shared views this call.
    deltas: u64,
}

impl ApplyReport {
    /// Returns the commit sequence assigned to this transaction.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the subscriptions touched by this commit, in ascending id
    /// order. "Touched" means the subscription's table appeared in the
    /// change set (or the subscription was marked stale); a touched
    /// subscription may have produced zero deltas when nothing matched.
    pub fn affected(&self) -> &[SubscriptionId] {
        &self.affected
    }

    /// Returns the number of `apply_change` evaluations performed — one per
    /// (change × distinct query) whose table appeared in the change set
    /// (ADR-020 D3). The Phase 20 headline metric: before grouping this was
    /// changes × subscriptions.
    pub fn evaluations(&self) -> u64 {
        self.evaluations
    }

    /// Returns the number of subscription updates produced by the shared
    /// views (before per-member fan-out).
    pub fn deltas(&self) -> u64 {
        self.deltas
    }
}

/// Cumulative registry statistics (ADR-020 D3), for benchmarking and
/// observability.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RegistryStats {
    /// Total `apply_change` evaluations: one per (change × distinct query).
    pub evaluations: u64,
    /// Total subscription updates produced by the shared views.
    pub deltas: u64,
    /// Total member buffer appends (deltas fanned out to members).
    pub fanouts: u64,
}

/// A borrowed view of one subscription: its per-member delivery state plus
/// the shared derived view of its query group (ADR-020 D1).
pub struct SubscriptionRef<'a> {
    subscription: &'a Subscription,
    view: &'a SharedView,
}

impl<'a> SubscriptionRef<'a> {
    /// Returns the subscription id.
    pub fn id(&self) -> SubscriptionId {
        self.subscription.id()
    }

    /// Returns the logical query.
    pub fn query(&self) -> &Query {
        self.subscription.query()
    }

    /// Returns the lifecycle state.
    pub fn state(&self) -> SubscriptionState {
        self.subscription.state()
    }

    /// Returns the observation cursor.
    pub fn cursor(&self) -> u64 {
        self.subscription.cursor()
    }

    /// Returns the number of pending, undelivered updates.
    pub fn buffer_len(&self) -> usize {
        self.subscription.buffer_len()
    }

    /// Returns the number of rows in the delivered view (from the shared
    /// view).
    pub fn visible_len(&self) -> usize {
        self.view.visible_len()
    }

    /// Returns the shared row payload in the window for `row_id`, if
    /// present. Test-only introspection proving payload sharing (ADR-019
    /// D4).
    #[cfg(test)]
    pub(crate) fn window_row(
        &self,
        row_id: nexum_core::RowId,
    ) -> Option<&std::sync::Arc<nexum_core::Row>> {
        self.view.window_row(row_id)
    }
}

/// The subscription registry.
#[derive(Debug)]
pub struct SubscriptionRegistry {
    subscriptions: BTreeMap<SubscriptionId, Subscription>,
    /// Shared derived views, one per distinct query (ADR-020 D1). `None`
    /// slots are free (freed when their last member unsubscribes).
    views: Vec<Option<SharedView>>,
    /// Member count per view slot (parallel to `views`).
    view_counts: Vec<usize>,
    next_id: u64,
    next_seq: u64,
    config: SubscriptionConfig,
    stats: RegistryStats,
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionRegistry {
    /// Creates an empty registry with default bounds.
    pub fn new() -> Self {
        Self {
            subscriptions: BTreeMap::new(),
            views: Vec::new(),
            view_counts: Vec::new(),
            next_id: 0,
            next_seq: 0,
            config: SubscriptionConfig::default(),
            stats: RegistryStats::default(),
        }
    }

    /// Creates an empty registry with explicit bounds.
    ///
    /// Returns [`Error::invalid_argument`] if any bound is zero.
    pub fn with_config(config: SubscriptionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            subscriptions: BTreeMap::new(),
            views: Vec::new(),
            view_counts: Vec::new(),
            next_id: 0,
            next_seq: 0,
            config,
            stats: RegistryStats::default(),
        })
    }

    /// Returns the enforced bounds.
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    /// The next commit sequence number: every `apply_changes` call consumes
    /// one, in commit order.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Returns the cumulative statistics (ADR-020 D3).
    pub fn stats(&self) -> &RegistryStats {
        &self.stats
    }

    /// Finds the shared view whose query equals `query`, if any.
    fn find_view(&self, query: &Query) -> Option<usize> {
        self.views
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|view| view.query() == query))
    }

    /// Establishes a subscription over `query` and returns its id.
    ///
    /// Compiles the query against the table schema (failing with
    /// [`Error::not_found`] for a missing table or [`Error::invalid_argument`]
    /// for unknown columns / type mismatches), captures the observation
    /// cursor, and delivers the `Initial` snapshot into the subscription's
    /// buffer.
    ///
    /// **Atomic establishment** (ADR-008 D4): the cursor is captured before
    /// the scan, and no commit can interleave in this exclusive-ownership
    /// model — every change committed before `subscribe` is in the snapshot,
    /// every change applied after is delivered live.
    ///
    /// **Grouping** (ADR-020 D1): an identical query joins the existing
    /// shared view instead of building a duplicate window. A live group's
    /// view is already current (kept up to date by `apply_changes`), so the
    /// new member snapshots it directly; an orphaned view (no members) is
    /// rebuilt from the authoritative scan first.
    pub fn subscribe(&mut self, store: &TableStore, query: Query) -> Result<SubscriptionId> {
        let id = SubscriptionId::from_u64(self.next_id);
        self.next_id += 1;
        let seq = self.next_seq; // observation point

        let view_idx = match self.find_view(&query) {
            Some(idx) => {
                if self.view_counts[idx] == 0 {
                    // Orphaned view: rebuild it so the window is current.
                    let compiled = compile(store, &query, &self.config)?;
                    let table = store
                        .table(query.table())
                        .expect("compiled against the same store");
                    let rows = matching_rows(table, &compiled);
                    let view = self.views[idx].as_mut().expect("found view");
                    view.set_compiled(compiled);
                    view.rebuild(rows);
                }
                idx
            }
            None => {
                let compiled = compile(store, &query, &self.config)?;
                let table = store
                    .table(query.table())
                    .expect("compiled against the same store");
                let rows = matching_rows(table, &compiled);
                let view = SharedView::new(query.clone(), compiled, self.config.max_snapshot_rows);
                let mut view = view;
                view.rebuild(rows);
                self.views.push(Some(view));
                self.view_counts.push(0);
                self.views.len() - 1
            }
        };

        self.view_counts[view_idx] += 1;
        let delivered = self.views[view_idx]
            .as_ref()
            .expect("view exists")
            .visible_rows();
        let mut subscription = Subscription::new(id, query, self.config.max_buffered, view_idx);
        subscription.receive_initial(seq, delivered);
        self.subscriptions.insert(id, subscription);
        Ok(id)
    }

    /// Fans one committed transaction's change set out to every affected
    /// subscription, atomically (ADR-008 D8): one call = one transaction,
    /// and all of its deltas reach each subscription's buffer in one
    /// synchronous pass, in deterministic order.
    ///
    /// **Grouped evaluation** (ADR-020 D1): each distinct query's shared
    /// view is evaluated once per change, producing the group's delta
    /// stream; each member then receives that stream in its own buffer.
    /// Identical queries therefore cost O(changes) evaluation + a
    /// window-sized per-member clone, instead of O(changes × subscriptions).
    ///
    /// `store` is consulted only to detect dropped tables (a subscription
    /// whose table no longer exists is marked stale). This is infallible:
    /// subscription processing never rejects a committed change (the commit
    /// already happened). Returns the assigned sequence number, the affected
    /// subscription ids (ascending), and the evaluation/delta counts.
    pub fn apply_changes(&mut self, store: &TableStore, changes: &[Change]) -> ApplyReport {
        let seq = self.next_seq;
        self.next_seq += 1;

        let changed_tables: BTreeSet<TableId> = changes.iter().map(Change::table_id).collect();
        // Table ids that exist right now, for drop detection.
        let existing: BTreeSet<TableId> = store.tables().map(|(_, table)| table.id()).collect();

        // Group members by shared view, in subscription id order.
        let mut by_view: BTreeMap<usize, Vec<SubscriptionId>> = BTreeMap::new();
        for (sid, sub) in &self.subscriptions {
            by_view.entry(sub.view()).or_default().push(*sid);
        }

        let mut affected = Vec::new();
        let mut evaluations = 0u64;
        let mut deltas = 0u64;
        let mut fanouts = 0u64;

        for (view_idx, members) in by_view {
            let dropped = self.views[view_idx]
                .as_ref()
                .is_none_or(|view| !existing.contains(&view.table_id()));
            if dropped {
                // The observed table was dropped: every member's view is
                // invalid. This is checked before the change filter so it
                // fires even when the commit touches only unrelated tables.
                for sid in &members {
                    let sub = self.subscriptions.get_mut(sid).expect("member exists");
                    if sub.state() == SubscriptionState::Stale {
                        continue;
                    }
                    sub.mark_stale(seq);
                    affected.push(*sid);
                }
                continue;
            }
            if !changed_tables.contains(
                &self.views[view_idx]
                    .as_ref()
                    .expect("referenced view exists")
                    .table_id(),
            ) {
                continue;
            }
            // Evaluate the shared view once per distinct query (ADR-020 D1).
            let view = self.views[view_idx]
                .as_mut()
                .expect("referenced view exists");
            let view_table = view.table_id();
            let mut scratch: Vec<SubscriptionUpdate> = Vec::new();
            for change in changes
                .iter()
                .filter(|change| change.table_id() == view_table)
            {
                view.apply_change(change, seq, &mut scratch);
                evaluations += 1;
            }
            deltas += scratch.len() as u64;
            // Fan the delta stream out to each member, in id order.
            for sid in &members {
                let sub = self.subscriptions.get_mut(sid).expect("member exists");
                if sub.state() == SubscriptionState::Stale {
                    continue;
                }
                sub.push_commit(&scratch, seq);
                fanouts += scratch.len() as u64;
                affected.push(*sid);
            }
        }

        self.stats.evaluations += evaluations;
        self.stats.deltas += deltas;
        self.stats.fanouts += fanouts;
        affected.sort_unstable();
        ApplyReport {
            seq,
            affected,
            evaluations,
            deltas,
        }
    }

    /// Returns `true` when the subscription has buffered updates waiting.
    pub fn has_pending(&self, id: SubscriptionId) -> Result<bool> {
        let subscription = self
            .subscriptions
            .get(&id)
            .ok_or_else(|| Error::not_found(format!("subscription {id} does not exist")))?;
        Ok(subscription.has_pending())
    }

    /// Takes the pending updates of one subscription, leaving its buffer
    /// empty. Returns [`Error::not_found`] for an unknown subscription.
    pub fn drain(&mut self, id: SubscriptionId) -> Result<Vec<SubscriptionUpdate>> {
        let subscription = self
            .subscriptions
            .get_mut(&id)
            .ok_or_else(|| Error::not_found(format!("subscription {id} does not exist")))?;
        Ok(subscription.take_buffer())
    }

    /// Drains ALL subscriptions that have pending updates in a single pass.
    /// Returns `(subscription_id, updates)` pairs.
    ///
    /// This is O(N) where N = total subscriptions, avoiding N separate
    /// BTreeMap lookups. At 20K subscriptions this eliminates ~300K comparison
    /// operations per tick (Phase 23-25 optimization).
    pub fn drain_all_pending(&mut self) -> Vec<(SubscriptionId, Vec<SubscriptionUpdate>)> {
        self.subscriptions
            .iter_mut()
            .filter_map(|(id, sub)| {
                let buf = sub.take_buffer();
                if buf.is_empty() {
                    None
                } else {
                    Some((*id, buf))
                }
            })
            .collect()
    }

    /// Ends a subscription and drops its delivery state. The shared view is
    /// freed when the last member leaves (ADR-020 D1).
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> Result<()> {
        let subscription = self
            .subscriptions
            .remove(&id)
            .ok_or_else(|| Error::not_found(format!("subscription {id} does not exist")))?;
        let view_idx = subscription.view();
        self.view_counts[view_idx] -= 1;
        if self.view_counts[view_idx] == 0 {
            self.views[view_idx] = None;
        }
        Ok(())
    }

    /// Returns a borrowed view of one subscription: delivery state plus the
    /// shared derived view of its query group.
    pub fn lookup(&self, id: SubscriptionId) -> Option<SubscriptionRef<'_>> {
        let subscription = self.subscriptions.get(&id)?;
        let view = self.views[subscription.view()].as_ref()?;
        Some(SubscriptionRef { subscription, view })
    }

    /// Returns `true` if the subscription is stale (fell behind or its table
    /// was dropped).
    pub fn is_stale(&self, id: SubscriptionId) -> Result<bool> {
        let subscription = self
            .subscriptions
            .get(&id)
            .ok_or_else(|| Error::not_found(format!("subscription {id} does not exist")))?;
        Ok(subscription.state() == SubscriptionState::Stale)
    }

    /// Regenerates the exact authoritative view of a subscription: rebuilds
    /// the shared derived view from a fresh scan, clears the delivery
    /// buffer, and returns the subscription to `Active` with a `Resync`
    /// update and a new cursor.
    ///
    /// Recompiles the query by name, so a subscription whose table was
    /// dropped and recreated re-attaches to the new table. The rebuilt view
    /// is shared with the query group (ADR-020 D1) but value-identical to
    /// its prior state, so other members are unaffected; only the resynced
    /// member receives the `Resync` update. Returns [`Error::not_found`] if
    /// the subscription — or its table — does not exist.
    pub fn resync(&mut self, store: &TableStore, id: SubscriptionId) -> Result<()> {
        let view_idx = self
            .subscriptions
            .get(&id)
            .ok_or_else(|| Error::not_found(format!("subscription {id} does not exist")))?
            .view();
        let query = self
            .subscriptions
            .get(&id)
            .expect("checked above")
            .query()
            .clone();
        let compiled = compile(store, &query, &self.config)?;
        let table = store
            .table(query.table())
            .expect("compiled against the same store");
        let rows = matching_rows(table, &compiled);
        let view = self.views[view_idx].as_mut().expect("member view exists");
        view.set_compiled(compiled);
        let seq = self.next_seq; // observation point
        let delivered = view.rebuild(rows);
        let subscription = self.subscriptions.get_mut(&id).expect("member exists");
        subscription.receive_resync(seq, delivered);
        Ok(())
    }

    /// Validates every subscription against the current store, marking stale
    /// any subscription whose observed table no longer exists. This is the
    /// on-demand drop-detection hook; `apply_changes` also performs the
    /// check on every commit. The `Stale` marker's sequence is the current
    /// observation point (`next_seq`), the same position semantics as
    /// `Initial`/`Resync`.
    pub fn refresh(&mut self, store: &TableStore) {
        let existing: BTreeSet<TableId> = store.tables().map(|(_, table)| table.id()).collect();
        for view_idx in 0..self.views.len() {
            let dropped = self.views[view_idx]
                .as_ref()
                .is_some_and(|view| !existing.contains(&view.table_id()));
            if !dropped {
                continue;
            }
            for sub in self.subscriptions.values_mut() {
                if sub.view() == view_idx && sub.state() != SubscriptionState::Stale {
                    sub.mark_stale(self.next_seq);
                }
            }
        }
    }

    /// Iterates over every subscription (member) in ascending id order.
    pub fn list(&self) -> impl Iterator<Item = &Subscription> {
        self.subscriptions.values()
    }

    /// Returns the number of subscriptions (members).
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns `true` if there are no subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Returns the number of distinct shared views (ADR-020 D1) — the
    /// interest-management fan-out factor: `apply_changes` evaluates each
    /// view once per change.
    pub fn view_count(&self) -> usize {
        self.views.iter().filter(|slot| slot.is_some()).count()
    }
}
