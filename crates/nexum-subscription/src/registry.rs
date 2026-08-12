//! The subscription registry (ADR-008).
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
use crate::subscription::{Subscription, SubscriptionState};

/// The result of one `apply_changes` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    seq: u64,
    affected: Vec<SubscriptionId>,
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
}

/// The subscription registry.
#[derive(Debug)]
pub struct SubscriptionRegistry {
    subscriptions: BTreeMap<SubscriptionId, Subscription>,
    next_id: u64,
    next_seq: u64,
    config: SubscriptionConfig,
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
            next_id: 0,
            next_seq: 0,
            config: SubscriptionConfig::default(),
        }
    }

    /// Creates an empty registry with explicit bounds.
    ///
    /// Returns [`Error::invalid_argument`] if any bound is zero.
    pub fn with_config(config: SubscriptionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            subscriptions: BTreeMap::new(),
            next_id: 0,
            next_seq: 0,
            config,
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

    /// Establishes a subscription over `query` and returns its id.
    ///
    /// Compiles the query against the table schema (failing with
    /// [`Error::not_found`] for a missing table or [`Error::invalid_argument`]
    /// for unknown columns / type mismatches), captures the observation
    /// cursor, scans the authoritative table, and delivers the `Initial`
    /// snapshot into the subscription's buffer.
    ///
    /// **Atomic establishment** (ADR-008 D4): the cursor is captured before
    /// the scan, and no commit can interleave in this exclusive-ownership
    /// model — every change committed before `subscribe` is in the snapshot,
    /// every change applied after is delivered live.
    pub fn subscribe(&mut self, store: &TableStore, query: Query) -> Result<SubscriptionId> {
        let compiled = compile(store, &query, &self.config)?;
        let id = SubscriptionId::from_u64(self.next_id);
        self.next_id += 1;
        let seq = self.next_seq; // observation point
        let table = store
            .table(query.table())
            .expect("compiled against the same store");
        let rows = matching_rows(table, &compiled);
        let mut subscription = Subscription::new(
            id,
            query,
            compiled,
            self.config.max_buffered,
            self.config.max_snapshot_rows,
        );
        let delivered = subscription.rebuild(rows);
        subscription.receive_initial(seq, delivered);
        self.subscriptions.insert(id, subscription);
        Ok(id)
    }

    /// Fans one committed transaction's change set out to every affected
    /// subscription, atomically (ADR-008 D8): one call = one transaction,
    /// and all of its deltas reach each subscription's buffer in one
    /// synchronous pass, in deterministic order.
    ///
    /// `store` is consulted only to detect dropped tables (a subscription
    /// whose table no longer exists is marked stale). This is infallible:
    /// subscription processing never rejects a committed change (the commit
    /// already happened). Returns the assigned sequence number and the
    /// affected subscription ids.
    pub fn apply_changes(&mut self, store: &TableStore, changes: &[Change]) -> ApplyReport {
        let seq = self.next_seq;
        self.next_seq += 1;

        let changed_tables: BTreeSet<TableId> =
            changes.iter().map(Change::table_id).collect();
        // Table ids that exist right now, for drop detection.
        let existing: BTreeSet<TableId> = store.tables().map(|(_, table)| table.id()).collect();

        let mut affected = Vec::new();
        for subscription in self.subscriptions.values_mut() {
            if subscription.state() == SubscriptionState::Stale {
                continue; // deltas are dropped while stale
            }
            let table_id = subscription.table_id();
            if !existing.contains(&table_id) {
                // The observed table was dropped: the view is invalid. This
                // is checked before the change filter so it fires even when
                // the commit touches only unrelated tables.
                subscription.mark_stale(seq);
                affected.push(subscription.id());
                continue;
            }
            if !changed_tables.contains(&table_id) {
                continue;
            }
            for change in changes.iter().filter(|change| change.table_id() == table_id) {
                subscription.apply_change(change, seq);
            }
            affected.push(subscription.id());
        }
        ApplyReport { seq, affected }
    }

    /// Takes the pending updates of one subscription, leaving its buffer
    /// empty. Returns [`Error::not_found`] for an unknown subscription.
    pub fn drain(&mut self, id: SubscriptionId) -> Result<Vec<SubscriptionUpdate>> {
        let subscription = self.subscriptions.get_mut(&id).ok_or_else(|| {
            Error::not_found(format!("subscription {id} does not exist"))
        })?;
        Ok(subscription.take_buffer())
    }

    /// Ends a subscription and drops its derived view and buffer.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> Result<()> {
        if self.subscriptions.remove(&id).is_none() {
            return Err(Error::not_found(format!(
                "subscription {id} does not exist"
            )));
        }
        Ok(())
    }

    /// Returns a subscription handle for introspection.
    pub fn lookup(&self, id: SubscriptionId) -> Option<&Subscription> {
        self.subscriptions.get(&id)
    }

    /// Returns `true` if the subscription is stale (fell behind or its table
    /// was dropped).
    pub fn is_stale(&self, id: SubscriptionId) -> Result<bool> {
        let subscription = self.subscriptions.get(&id).ok_or_else(|| {
            Error::not_found(format!("subscription {id} does not exist"))
        })?;
        Ok(subscription.state() == SubscriptionState::Stale)
    }

    /// Regenerates the exact authoritative view of a subscription: rebuilds
    /// the derived cache from a fresh scan, clears the delivery buffer, and
    /// returns the subscription to `Active` with a `Resync` update and a new
    /// cursor.
    ///
    /// Recompiles the query by name, so a subscription whose table was
    /// dropped and recreated re-attaches to the new table. Returns
    /// [`Error::not_found`] if the subscription — or its table — does not
    /// exist.
    pub fn resync(&mut self, store: &TableStore, id: SubscriptionId) -> Result<()> {
        let subscription = self.subscriptions.get_mut(&id).ok_or_else(|| {
            Error::not_found(format!("subscription {id} does not exist"))
        })?;
        let compiled = compile(store, subscription.query(), &self.config)?;
        let table = store
            .table(subscription.query().table())
            .expect("compiled against the same store");
        let rows = matching_rows(table, &compiled);
        subscription.set_compiled(compiled);
        let seq = self.next_seq; // observation point
        let delivered = subscription.rebuild(rows);
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
        for subscription in self.subscriptions.values_mut() {
            if subscription.state() == SubscriptionState::Stale {
                continue;
            }
            if !existing.contains(&subscription.table_id()) {
                subscription.mark_stale(self.next_seq);
            }
        }
    }

    /// Iterates over every subscription in ascending id order.
    pub fn list(&self) -> impl Iterator<Item = &Subscription> {
        self.subscriptions.values()
    }

    /// Returns the number of subscriptions.
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns `true` if there are no subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

}
