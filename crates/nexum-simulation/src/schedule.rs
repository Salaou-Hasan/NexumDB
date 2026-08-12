//! Deterministic scheduled events ([`Schedule`], [`ScheduledEvent`]).
//!
//! Future simulation actions are scheduled by **logical tick**, never by
//! wall-clock timers: an event scheduled for tick N runs at the start of
//! tick N (or the next tick, if N was skipped), in `(at_tick, id)` order
//! (ADR-009 D4, design §12). An event invokes a named reducer against the
//! tick's transaction, so scheduled actions are just reducers with a future
//! execution time.

use nexum_core::{Error, Result, TickId};
use nexum_reducer::ReducerArgs;

/// One scheduled future action: invoke `reducer` with `args` at `at_tick`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledEvent {
    id: u64,
    at_tick: TickId,
    reducer: String,
    args: ReducerArgs,
}

impl ScheduledEvent {
    /// Returns the unique event id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the tick at which the event becomes due.
    pub fn at_tick(&self) -> TickId {
        self.at_tick
    }

    /// Returns the name of the reducer the event invokes.
    pub fn reducer(&self) -> &str {
        &self.reducer
    }

    /// Returns the reducer arguments.
    pub fn args(&self) -> &ReducerArgs {
        &self.args
    }
}

/// A bounded, deterministic collection of scheduled events.
#[derive(Debug)]
pub struct Schedule {
    /// Always sorted by `(at_tick, id)`.
    events: Vec<ScheduledEvent>,
    next_id: u64,
    max: usize,
}

impl Schedule {
    /// Creates an empty schedule bounded to `max` pending events.
    pub fn new(max: usize) -> Self {
        Self {
            events: Vec::new(),
            next_id: 0,
            max,
        }
    }

    /// Schedules an event at `at_tick` and returns its unique id.
    ///
    /// Returns `Capacity` when the schedule is full and `InvalidArgument`
    /// for an empty reducer name.
    pub fn schedule(
        &mut self,
        at_tick: TickId,
        reducer: impl Into<String>,
        args: ReducerArgs,
    ) -> Result<u64> {
        let reducer = reducer.into();
        if reducer.is_empty() {
            return Err(Error::invalid_argument(
                "scheduled reducer name must not be empty",
            ));
        }
        if self.events.len() >= self.max {
            return Err(Error::capacity(format!(
                "schedule is full (max {} pending events)",
                self.max
            )));
        }
        let id = self.next_id;
        self.next_id += 1;
        self.events.push(ScheduledEvent {
            id,
            at_tick,
            reducer,
            args,
        });
        // Deterministic execution order regardless of insertion order.
        self.events.sort_by_key(|event| (event.at_tick, event.id));
        Ok(id)
    }

    /// Cancels a pending event. Returns `NotFound` if it does not exist.
    pub fn cancel(&mut self, id: u64) -> Result<()> {
        match self.events.iter().position(|event| event.id == id) {
            Some(position) => {
                self.events.remove(position);
                Ok(())
            }
            None => Err(Error::not_found(format!(
                "scheduled event {id} does not exist"
            ))),
        }
    }

    /// Takes every event due at or before `tick`, in `(at_tick, id)` order.
    ///
    /// Events scheduled for a skipped earlier tick are simply run at the
    /// first tick that executes at or after their target — logical, never
    /// wall-clock, semantics.
    pub fn take_due(&mut self, tick: TickId) -> Vec<ScheduledEvent> {
        let split = self.events.partition_point(|event| event.at_tick <= tick);
        self.events.drain(..split).collect()
    }

    /// Returns the pending events in `(at_tick, id)` order.
    pub fn pending(&self) -> &[ScheduledEvent] {
        &self.events
    }

    /// Returns the number of pending events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns `true` if no events are pending.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn due_events_run_in_target_tick_order() {
        let mut schedule = Schedule::new(16);
        schedule
            .schedule(
                TickId::from_u64(5),
                "late",
                ReducerArgs::new().insert("n", 3u64),
            )
            .unwrap();
        schedule
            .schedule(
                TickId::from_u64(3),
                "early",
                ReducerArgs::new().insert("n", 1u64),
            )
            .unwrap();
        schedule
            .schedule(
                TickId::from_u64(3),
                "also_early",
                ReducerArgs::new().insert("n", 2u64),
            )
            .unwrap();

        // Nothing due before tick 3.
        assert!(schedule.take_due(TickId::from_u64(2)).is_empty());
        let due = schedule.take_due(TickId::from_u64(3));
        let reducers: Vec<&str> = due.iter().map(ScheduledEvent::reducer).collect();
        assert_eq!(reducers, vec!["early", "also_early"]);
        assert_eq!(schedule.pending().len(), 1);
    }

    #[test]
    fn overdue_events_run_at_next_tick() {
        let mut schedule = Schedule::new(4);
        schedule
            .schedule(TickId::from_u64(1), "jump", ReducerArgs::new())
            .unwrap();
        // Tick 0: nothing due yet.
        assert!(schedule.take_due(TickId::from_u64(0)).is_empty());
        // Tick 2 executes but tick 1 never ran: the overdue event is due now
        // (at_tick <= 2) and runs at the first executed tick at or after its
        // target — logical, never wall-clock, semantics.
        let due = schedule.take_due(TickId::from_u64(2));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].reducer(), "jump");
        assert!(schedule.is_empty());
    }

    #[test]
    fn cancel_removes_pending_events() {
        let mut schedule = Schedule::new(4);
        let id = schedule
            .schedule(TickId::from_u64(3), "boom", ReducerArgs::new())
            .unwrap();
        schedule.cancel(id).unwrap();
        assert!(schedule.is_empty());
        assert!(schedule.cancel(id).is_err());
    }

    #[test]
    fn capacity_is_bounded() {
        let mut schedule = Schedule::new(2);
        schedule
            .schedule(TickId::from_u64(1), "a", ReducerArgs::new())
            .unwrap();
        schedule
            .schedule(TickId::from_u64(1), "b", ReducerArgs::new())
            .unwrap();
        assert!(schedule.schedule(TickId::from_u64(1), "c", ReducerArgs::new()).is_err());
    }

    #[test]
    fn empty_reducer_name_is_rejected() {
        let mut schedule = Schedule::new(4);
        assert!(schedule.schedule(TickId::from_u64(1), "", ReducerArgs::new()).is_err());
    }
}
