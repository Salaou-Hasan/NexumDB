//! SDK unit tests (ADR-013): view logic, the connection state machine,
//! request correlation, and client-side bounds.

use std::sync::Arc;

use nexum_core::{RowId, SubscriptionId, TickId, Value, WorldId};
use nexum_network::auth::Principal;
use nexum_network::transport::{Connection, TransportError};
use nexum_subscription::DeliveredRow;

use crate::connection::ConnectionState;
use crate::error::SdkError;
use crate::event::ServerEvent;
use crate::protocol::{DeltaKind, PROTOCOL_VERSION, ServerMessage};
use crate::request::PendingCall;
use crate::session::SessionInfo;
use crate::subscription::SubscriptionHandle;
use crate::transport::ClientTransport;
use crate::view::View;
use crate::{Client, SdkConfig};

/// A transport that buffers a fixed inbound queue and accepts outbound
/// frames — used to exercise the client's pump, guards, and dispatch
/// without a real link.
struct TestConnection {
    peer: String,
    closed: bool,
    inbound: std::collections::VecDeque<Vec<u8>>,
}

impl TestConnection {
    fn new() -> Self {
        Self {
            peer: "test".into(),
            closed: false,
            inbound: std::collections::VecDeque::new(),
        }
    }

    fn with_frames(frames: Vec<Vec<u8>>) -> Self {
        Self {
            peer: "test".into(),
            closed: false,
            inbound: frames.into(),
        }
    }
}

impl Connection for TestConnection {
    fn peer(&self) -> &str {
        &self.peer
    }
    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        Ok(self.inbound.pop_front().map(Arc::from))
    }
    fn try_send_frame(&mut self, _frame: Arc<[u8]>) -> Result<(), TransportError> {
        if self.closed {
            Err(TransportError::Closed)
        } else {
            Ok(())
        }
    }
    fn flush_outbound(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
    fn close(&mut self) {
        self.closed = true;
    }
}

fn dummy_connection() -> Box<dyn Connection> {
    Box::new(TestConnection::new())
}

/// A link that reports `Closed` on every operation (a broken TCP link).
struct BrokenConnection;

impl Connection for BrokenConnection {
    fn peer(&self) -> &str {
        "broken"
    }
    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
        Err(TransportError::Closed)
    }
    fn try_send_frame(&mut self, _frame: Arc<[u8]>) -> Result<(), TransportError> {
        Err(TransportError::Closed)
    }
    fn flush_outbound(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
    fn close(&mut self) {}
}

fn row_a() -> DeliveredRow {
    DeliveredRow::new(RowId::from_u64(1), nexum_core::row![1u64, 10u64, 100i32])
}

fn row_b() -> DeliveredRow {
    DeliveredRow::new(RowId::from_u64(2), nexum_core::row![2u64, 20u64, 90i32])
}

/// A client whose session is authenticated and attached to world 0.
fn attached_client() -> Client {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.state = ConnectionState::Connected;
    client.session = Some(SessionInfo::new(Principal::new(1, "alice")));
    client
        .session
        .as_mut()
        .unwrap()
        .attach(WorldId::from_u64(0));
    client.transport = Some(ClientTransport::new(dummy_connection()));
    client
}

// ---------------------------------------------------------------- view

#[test]
fn view_applies_snapshot_and_contiguous_deltas() {
    let mut view = View::new();
    view.apply_snapshot(0, vec![row_a(), row_b()]);
    assert_eq!(view.len(), 2);
    assert!(view.contains(RowId::from_u64(1)));
    assert_eq!(view.seq(), 0);

    // Insert a new row (seq 1).
    view.apply_delta(
        1,
        DeltaKind::Insert,
        RowId::from_u64(3),
        Some(std::sync::Arc::new(DeliveredRow::new(
            RowId::from_u64(3),
            nexum_core::row![3u64, 30u64, 80i32],
        ))),
    )
    .unwrap();
    assert_eq!(view.len(), 3);
    assert_eq!(view.seq(), 1);

    // Update an existing row (seq 2).
    view.apply_delta(
        2,
        DeltaKind::Update,
        RowId::from_u64(1),
        Some(std::sync::Arc::new(DeliveredRow::new(
            RowId::from_u64(1),
            nexum_core::row![1u64, 10u64, 1i32],
        ))),
    )
    .unwrap();
    assert_eq!(
        view.get(RowId::from_u64(1)).unwrap().row().get(2),
        Some(&Value::I32(1))
    );

    // Delete a row (seq 3).
    view.apply_delta(3, DeltaKind::Delete, RowId::from_u64(2), None)
        .unwrap();
    assert_eq!(view.len(), 2);
    assert!(!view.contains(RowId::from_u64(2)));
    assert_eq!(view.seq(), 3);
}

#[test]
fn view_detects_sequence_gaps() {
    let mut view = View::new();
    view.apply_snapshot(0, vec![]);
    // A commit that skipped sequences means missed commits.
    let err = view
        .apply_delta(
            2,
            DeltaKind::Insert,
            RowId::from_u64(1),
            Some(std::sync::Arc::new(row_a())),
        )
        .unwrap_err();
    assert_eq!(err.expected, 1);
    assert_eq!(err.got, 2);
    // The first commit sits at the observation point (seq == snapshot seq).
    view.apply_delta(
        0,
        DeltaKind::Insert,
        RowId::from_u64(1),
        Some(std::sync::Arc::new(row_a())),
    )
    .unwrap();
    // Several deltas of the same commit share its sequence.
    view.apply_delta(
        0,
        DeltaKind::Insert,
        RowId::from_u64(2),
        Some(std::sync::Arc::new(row_b())),
    )
    .unwrap();
    // The next commit advances the cursor by one.
    view.apply_delta(
        1,
        DeltaKind::Insert,
        RowId::from_u64(3),
        Some(std::sync::Arc::new(row_b())),
    )
    .unwrap();
    assert_eq!(view.seq(), 1);
    // A delta from an older commit after the cursor advanced is a reorder.
    let err = view
        .apply_delta(0, DeltaKind::Delete, RowId::from_u64(1), None)
        .unwrap_err();
    assert_eq!(err.expected, 2);
    assert_eq!(err.got, 0);
    // A commit that skipped sequences is a gap.
    let err = view
        .apply_delta(3, DeltaKind::Delete, RowId::from_u64(1), None)
        .unwrap_err();
    assert_eq!(err.expected, 2);
    assert_eq!(err.got, 3);
}

#[test]
fn view_snapshot_replaces_the_whole_view() {
    let mut view = View::new();
    view.apply_snapshot(0, vec![row_a(), row_b()]);
    view.apply_snapshot(9, vec![row_a()]);
    assert_eq!(view.len(), 1);
    assert_eq!(view.seq(), 9);
    assert!(!view.contains(RowId::from_u64(2)));
}

// ------------------------------------------------------- state machine

#[test]
fn handshake_response_connects_the_client() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    assert_eq!(client.state(), ConnectionState::Disconnected);
    client.dispatch(ServerMessage::HandshakeResponse {
        version: PROTOCOL_VERSION,
        server_name: "nexum".into(),
    });
    assert!(client.is_connected());
    assert_eq!(
        client.take_events(),
        vec![ServerEvent::Connected {
            version: PROTOCOL_VERSION,
            server_name: "nexum".into(),
        }]
    );
}

#[test]
fn handshake_mismatch_closes_the_client() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.dispatch(ServerMessage::HandshakeResponse {
        version: PROTOCOL_VERSION + 1,
        server_name: "nexum".into(),
    });
    assert_eq!(client.state(), ConnectionState::Closed);
    assert!(matches!(
        client.last_error(),
        Some(SdkError::HandshakeMismatch { .. })
    ));
}

#[test]
fn connect_installs_the_transport_and_issues_the_handshake() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(dummy_connection()).unwrap();
    assert_eq!(client.state(), ConnectionState::Connecting);
    // A second connect is rejected.
    let err = client.connect(dummy_connection()).unwrap_err();
    assert!(matches!(err, SdkError::AlreadyConnected));
}

#[test]
fn api_guards_reject_operations_before_connect() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    let err = client
        .send_input(nexum_simulation::InputFrame::new(TickId::from_u64(0)))
        .unwrap_err();
    assert!(matches!(err, SdkError::NotConnected));
    let err = client
        .call_reducer("bump", nexum_reducer::ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, SdkError::NotConnected));
    let err = client
        .subscribe(
            nexum_subscription::Query::builder("players")
                .build()
                .unwrap(),
        )
        .unwrap_err();
    assert!(matches!(err, SdkError::NotConnected));
}

#[test]
fn api_guards_reject_operations_without_a_session() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.state = ConnectionState::Connected;
    client.transport = Some(ClientTransport::new(dummy_connection()));
    let err = client
        .send_input(nexum_simulation::InputFrame::new(TickId::from_u64(0)))
        .unwrap_err();
    assert!(matches!(err, SdkError::AuthenticationRequired));
}

#[test]
fn api_guards_reject_operations_without_an_attachment() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.state = ConnectionState::Connected;
    client.session = Some(SessionInfo::new(Principal::new(1, "alice")));
    client.transport = Some(ClientTransport::new(dummy_connection()));
    let err = client
        .send_input(nexum_simulation::InputFrame::new(TickId::from_u64(0)))
        .unwrap_err();
    assert!(matches!(err, SdkError::NotAttached));
    let err = client
        .call_reducer("bump", nexum_reducer::ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, SdkError::NotAttached));
}

#[test]
fn empty_reducer_names_are_rejected_locally() {
    let mut client = attached_client();
    let err = client
        .call_reducer("", nexum_reducer::ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, SdkError::InvalidArgument(_)));
}

#[test]
fn pending_call_limit_is_enforced_locally() {
    let config = SdkConfig::new().with_max_pending_calls(1);
    let mut client = Client::new(config).unwrap();
    client.state = ConnectionState::Connected;
    client.session = Some(SessionInfo::new(Principal::new(1, "alice")));
    client
        .session
        .as_mut()
        .unwrap()
        .attach(WorldId::from_u64(0));
    client.transport = Some(ClientTransport::new(dummy_connection()));
    client
        .call_reducer("bump", nexum_reducer::ReducerArgs::new())
        .unwrap();
    let err = client
        .call_reducer("bump", nexum_reducer::ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, SdkError::PendingCallLimit));
}

#[test]
fn input_frames_beyond_the_command_bound_are_rejected() {
    let config = SdkConfig::new().with_max_commands_per_frame(1);
    let mut client = Client::new(config).unwrap();
    client.state = ConnectionState::Connected;
    client.session = Some(SessionInfo::new(Principal::new(1, "alice")));
    client
        .session
        .as_mut()
        .unwrap()
        .attach(WorldId::from_u64(0));
    client.transport = Some(ClientTransport::new(dummy_connection()));
    let mut frame = nexum_simulation::InputFrame::new(TickId::from_u64(0));
    frame.push(nexum_simulation::InputCommand::new(1, "a", None).unwrap());
    frame.push(nexum_simulation::InputCommand::new(1, "b", None).unwrap());
    let err = client.send_input(frame).unwrap_err();
    assert!(matches!(err, SdkError::InvalidArgument(_)));
}

// ------------------------------------------------------------- correlation

#[test]
fn reducer_result_is_correlated_by_request_id() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.pending_calls.insert(7, PendingCall::new(7, "bump"));
    client.dispatch(ServerMessage::ReducerResult {
        request_id: 7,
        ok: true,
        value: Some(Value::U64(42)),
        error: None,
    });
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    assert_eq!(results[0].value(), Some(&Value::U64(42)));
    assert_eq!(client.pending_call_count(), 0);

    // An unknown request id surfaces as an error event, never a panic.
    client.dispatch(ServerMessage::ReducerResult {
        request_id: 99,
        ok: false,
        value: None,
        error: Some("nope".into()),
    });
    assert!(client.take_reducer_results().is_empty());
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::Error { code: 23, .. }]
    ));
}

#[test]
fn subscription_binds_by_request_id_and_tracks_deltas() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    let local = client.next_local_subscription;
    client.next_local_subscription += 1;
    client.pending_subscribes.insert(5, local);
    client
        .subscriptions
        .insert(local, SubscriptionHandle::new(local));
    client.views.insert(local, View::new());

    client.dispatch(ServerMessage::SubscriptionSnapshot {
        request_id: 5,
        subscription: SubscriptionId::from_u64(1),
        seq: 0,
        rows: vec![row_a()],
    });
    assert_eq!(client.pending_subscribes.len(), 0);
    let handle = client.subscription(local).unwrap();
    assert_eq!(handle.server(), Some(SubscriptionId::from_u64(1)));
    assert_eq!(client.view(local).unwrap().len(), 1);
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::SubscriptionBound { local: l, server, .. }]
            if *l == local && *server == SubscriptionId::from_u64(1)
    ));

    // A delta for the bound subscription updates the view.
    client.dispatch(ServerMessage::SubscriptionDelta {
        subscription: SubscriptionId::from_u64(1),
        seq: 1,
        kind: DeltaKind::Insert,
        row_id: RowId::from_u64(2),
        row: Some(row_b()),
    });
    assert_eq!(client.view(local).unwrap().len(), 2);
}

#[test]
fn rejected_subscribe_is_correlated_by_request_id() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    let local = client.next_local_subscription;
    client.next_local_subscription += 1;
    client.pending_subscribes.insert(5, local);
    client
        .subscriptions
        .insert(local, SubscriptionHandle::new(local));
    client.views.insert(local, View::new());

    client.dispatch(ServerMessage::Error {
        code: 17,
        message: "subscription limit reached".into(),
        request_id: 5,
    });
    assert!(client.subscription(local).is_none());
    assert!(client.view(local).is_none());
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::SubscriptionRejected { local: l, .. }] if *l == local
    ));
}

#[test]
fn stale_then_resync_restores_the_view() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    let local = 0u64;
    let mut handle = SubscriptionHandle::new(local);
    handle.bind(SubscriptionId::from_u64(3));
    client.subscriptions.insert(local, handle);
    let mut view = View::new();
    view.apply_snapshot(0, vec![row_a()]);
    client.views.insert(local, view);

    client.dispatch(ServerMessage::StaleNotification {
        subscription: SubscriptionId::from_u64(3),
        seq: 4,
    });
    assert!(client.subscription(local).unwrap().is_stale());

    // A resync snapshot refreshes the view and clears the stale mark.
    client.dispatch(ServerMessage::SubscriptionSnapshot {
        request_id: 0,
        subscription: SubscriptionId::from_u64(3),
        seq: 10,
        rows: vec![row_a(), row_b()],
    });
    assert!(!client.subscription(local).unwrap().is_stale());
    assert_eq!(client.view(local).unwrap().len(), 2);
    assert!(matches!(
        client.take_events().as_slice(),
        [
            ServerEvent::Stale { .. },
            ServerEvent::SubscriptionResynced { seq: 10, .. }
        ]
    ));
}

#[test]
fn view_gap_marks_the_handle_stale() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    let local = 0u64;
    let mut handle = SubscriptionHandle::new(local);
    handle.bind(SubscriptionId::from_u64(4));
    client.subscriptions.insert(local, handle);
    client.views.insert(local, View::new());

    client.dispatch(ServerMessage::SubscriptionSnapshot {
        request_id: 0,
        subscription: SubscriptionId::from_u64(4),
        seq: 0,
        rows: vec![],
    });
    client.dispatch(ServerMessage::SubscriptionDelta {
        subscription: SubscriptionId::from_u64(4),
        seq: 3, // gap: expected 1
        kind: DeltaKind::Insert,
        row_id: RowId::from_u64(1),
        row: Some(row_a()),
    });
    assert!(client.subscription(local).unwrap().is_stale());
    assert!(matches!(
        client.take_events().as_slice(),
        [
            ServerEvent::SubscriptionResynced { .. },
            ServerEvent::ViewGap {
                expected: 1,
                got: 3,
                ..
            }
        ]
    ));
}

// ---------------------------------------------------------------- failure

#[test]
fn disconnect_fails_pending_calls_with_correlated_results() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.pending_calls.insert(3, PendingCall::new(3, "bump"));
    client.dispatch(ServerMessage::Disconnect {
        reason: "server restart".into(),
    });
    assert_eq!(client.state(), ConnectionState::Closed);
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert!(!results[0].is_ok());
    assert!(results[0].error().unwrap().contains("server restart"));
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::Disconnected { .. }]
    ));
}

#[test]
fn malformed_server_frames_are_events_not_panics() {
    // A server frame that fails decoding (bad magic) must surface as an
    // error event and keep the client alive — never a panic.
    let connection = TestConnection::with_frames(vec![b"NOTAFRAME!!".to_vec()]);
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(Box::new(connection)).unwrap();
    let report = client.pump().unwrap();
    assert_eq!(report.frames, 1);
    assert_eq!(report.rejected, 1);
    assert!(!report.closed);
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::Error { code: 1, .. }] // ProtocolError::BadMagic = 1
    ));
}

#[test]
fn pump_treats_a_broken_transport_as_a_clean_disconnect() {
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.transport = Some(ClientTransport::new(Box::new(BrokenConnection)));
    client.pending_calls.insert(1, PendingCall::new(1, "bump"));
    let report = client.pump().unwrap();
    assert!(report.closed);
    assert_eq!(client.state(), ConnectionState::Closed);
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::Disconnected { .. }]
    ));
    // Pending calls fail with correlated results — never a hang.
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert!(!results[0].is_ok());
}

#[test]
fn events_are_bounded() {
    let config = SdkConfig::new().with_max_events(2);
    let mut client = Client::new(config).unwrap();
    client.push_event(ServerEvent::Pong { nonce: 1 });
    client.push_event(ServerEvent::Pong { nonce: 2 });
    client.push_event(ServerEvent::Pong { nonce: 3 });
    let events = client.take_events();
    assert_eq!(
        events,
        vec![
            ServerEvent::Pong { nonce: 2 },
            ServerEvent::Pong { nonce: 3 }
        ]
    );
}

#[test]
fn send_frame_flushes_the_outbound_transport() {
    // `ClientTransport::send_frame` must push buffered bytes to the
    // transport immediately (TCP correctness): queue transports flush
    // trivially, so a recording connection proves the flush is invoked.
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Recording {
        flushes: Arc<AtomicUsize>,
        queue: std::collections::VecDeque<Vec<u8>>,
    }
    impl Connection for Recording {
        fn peer(&self) -> &str {
            "recording"
        }
        fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
            Ok(self.queue.pop_front().map(Arc::from))
        }
        fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError> {
            self.queue.push_back(frame.to_vec());
            Ok(())
        }
        fn flush_outbound(&mut self) -> Result<(), TransportError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn close(&mut self) {}
    }

    let flushes = Arc::new(AtomicUsize::new(0));
    let mut transport = ClientTransport::new(Box::new(Recording {
        flushes: Arc::clone(&flushes),
        queue: std::collections::VecDeque::new(),
    }));
    transport.send_frame(vec![1, 2, 3]).unwrap();
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        1,
        "send_frame flushed the buffered frame to the transport"
    );
    transport.send_frame(vec![4, 5]).unwrap();
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        2,
        "every send flushes; a stream transport never holds bytes back"
    );
}
