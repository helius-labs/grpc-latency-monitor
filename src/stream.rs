//! Independent Yellowstone stream reader; no matching or storage work happens here.

use crate::{
    config::{FiltersConfig, ProviderConfig},
    model::{Arrival, Commitment, DataType, EventKey, StreamKind},
};
use futures::{SinkExt, StreamExt};
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
};

/// Log only error classifications: server-supplied messages can echo credentials.
fn connection_error_detail(error: &(dyn std::error::Error + 'static)) -> String {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(status) = error.downcast_ref::<yellowstone_grpc_proto::tonic::Status>() {
            return format!("gRPC status: {:?}", status.code());
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            return format!("I/O error: {:?}", error.kind());
        }
        current = error.source();
    }
    "unclassified failure (error text omitted to protect credentials)".into()
}

fn connection_failure(
    provider: &ProviderConfig,
    kind: StreamKind,
    reason: &'static str,
    error: &(dyn std::error::Error + 'static),
) -> &'static str {
    tracing::warn!(
        provider = %provider.name,
        data_type = kind.data_type.as_str(),
        commitment = kind.commitment.as_str(),
        reason,
        detail = %connection_error_detail(error),
        "provider operation failed"
    );
    reason
}
const FILTER_KEY: &str = "benchmark";

/// Backoff between consecutive failed attempts. The cap keeps a long outage from
/// being retried constantly; a recorded outage can then end up to this long after
/// the provider's recovery.
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
/// A subscription that stayed up this long reconnects immediately and resets the backoff.
const STABLE_SUBSCRIPTION: Duration = Duration::from_secs(30);
/// HTTP/2 PING cadence and acknowledgement deadline: detects dead TCP paths that
/// would otherwise leave a provider "connected" (and accruing misses) indefinitely.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Yellowstone answers client pings with Pong, so even a quiet filtered stream
/// receives traffic. No update, ping or pong for `IDLE_TIMEOUT` means a stall.
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// tonic's default is 4 MiB; Solana accounts can hold up to 10 MiB of data, and an
/// oversized update would fail decoding and force a reconnect on every attempt.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// Disconnect reason for a local backlog (the matcher could not keep up), not a
/// provider failure. Dashboards exclude it from provider downtime by this text.
pub const ARRIVAL_QUEUE_FULL: &str = "arrival queue full";
/// A fresh subscription dropped because the matcher could not accept its
/// connection yet (also a local backlog).
pub const CONTROL_QUEUE_FULL: &str = "control queue full";
/// Disconnects caused by the subscriber itself: excluded from provider downtime,
/// and never followed by an immediate reconnect into the same backlog.
pub const LOCAL_DISCONNECTS: [&str; 2] = [ARRIVAL_QUEUE_FULL, CONTROL_QUEUE_FULL];

/// Slot values: idle, publishing, or `2 + nanoseconds since base` of the stamp.
const IDLE: u64 = 0;
const STAMPING: u64 = 1;
/// Bound on waiting for a reader between publishing and recording its stamp.
const MAX_STAMPING_SPINS: u32 = 10_000;

/// Per-reader publication of updates and controls that have been stamped but
/// not yet enqueued, shared by the readers of one stream and its matcher.
///
/// A reader publishes before reading the clock and clears after enqueueing.
/// The matcher checks publications BEFORE draining its queues, so an update
/// stamped before a close cutoff was either still published at the check or
/// was enqueued before the drain. Unlike a busy counter, a stamp taken
/// microseconds ago never blocks a close; only a reader descheduled for longer
/// than the processing grace does. Each slot is written by one reader only.
pub struct InFlight {
    base: Instant,
    slots: Vec<AtomicU64>,
}

impl InFlight {
    pub fn new(readers: usize) -> Self {
        Self {
            base: Instant::now(),
            slots: (0..readers).map(|_| AtomicU64::new(IDLE)).collect(),
        }
    }

    /// Publish, then take, a stamp for one update or control. A nested stamp of
    /// the same reader keeps the outer (older) publication until it is dropped.
    pub fn stamp(&self, reader: usize) -> Stamp<'_> {
        let slot = &self.slots[reader];
        if slot.load(Ordering::SeqCst) != IDLE {
            return Stamp {
                slot: None,
                at: Instant::now(),
            };
        }
        slot.store(STAMPING, Ordering::SeqCst);
        let at = Instant::now();
        let since_base = u64::try_from(at.saturating_duration_since(self.base).as_nanos())
            .unwrap_or(u64::MAX - STAMPING);
        slot.store(since_base.saturating_add(STAMPING + 1), Ordering::SeqCst);
        Stamp {
            slot: Some(slot),
            at,
        }
    }

    /// Whether any reader holds a stamp taken before `cutoff` that it has not
    /// enqueued yet. A publication whose stamp cannot be read promptly counts.
    pub fn published_before(&self, cutoff: Instant) -> bool {
        self.slots.iter().any(|slot| {
            let mut value = slot.load(Ordering::SeqCst);
            let mut spins = 0;
            while value == STAMPING {
                if spins == MAX_STAMPING_SPINS {
                    return true;
                }
                std::hint::spin_loop();
                spins += 1;
                value = slot.load(Ordering::SeqCst);
            }
            value != IDLE && self.base + Duration::from_nanos(value - STAMPING - 1) < cutoff
        })
    }
}

/// Clears its reader's publication when dropped (after enqueueing or abandoning
/// the stamped item, including when a reader task is cancelled).
pub struct Stamp<'a> {
    slot: Option<&'a AtomicU64>,
    at: Instant,
}

impl Stamp<'_> {
    pub fn at(&self) -> Instant {
        self.at
    }
}

impl Drop for Stamp<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            slot.store(IDLE, Ordering::SeqCst);
        }
    }
}

/// The delay before reconnecting and the backoff after that. A subscription that
/// stayed up long enough reconnects immediately, so a provider's rolling restart
/// costs only the reconnect itself rather than our wait; repeated quick failures
/// keep backing off.
fn retry_delay(backoff: Duration, subscribed_for: Option<Duration>) -> (Duration, Duration) {
    if subscribed_for.is_some_and(|duration| duration >= STABLE_SUBSCRIPTION) {
        (Duration::ZERO, INITIAL_RETRY_DELAY)
    } else {
        (backoff, (backoff * 2).min(MAX_RETRY_DELAY))
    }
}

/// Like `retry_delay`, but a local backlog is not a provider restart: never
/// reconnect into it immediately, however long the subscription lasted.
fn retry_after(backoff: Duration, disconnect: &Disconnect) -> (Duration, Duration) {
    let local = LOCAL_DISCONNECTS.contains(&disconnect.reason);
    retry_delay(backoff, disconnect.subscribed_for.filter(|_| !local))
}

/// One data type per request. Filters are shared across providers and commitments.
pub fn subscribe_request(kind: StreamKind, filters: &FiltersConfig) -> SubscribeRequest {
    let filters = filters.normalized();
    let mut request = SubscribeRequest {
        commitment: Some(match kind.commitment {
            Commitment::Processed => CommitmentLevel::Processed as i32,
            Commitment::Finalized => CommitmentLevel::Finalized as i32,
        }),
        ..Default::default()
    };
    match kind.data_type {
        DataType::Account => {
            request.accounts.insert(
                FILTER_KEY.into(),
                SubscribeRequestFilterAccounts {
                    owner: filters.accounts_owner,
                    nonempty_txn_signature: Some(true),
                    ..Default::default()
                },
            );
        }
        DataType::Transaction => {
            request.transactions.insert(
                FILTER_KEY.into(),
                SubscribeRequestFilterTransactions {
                    account_include: filters.transactions_account_include,
                    ..Default::default()
                },
            );
        }
        DataType::Slot => {
            // One subscription contains every slot status, including interslot stages.
            // Events are keyed by (slot, status), so each stage is compared separately.
            request.slots.insert(
                FILTER_KEY.into(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            );
        }
    }
    request
}

/// `None` is only returned for Yellowstone keepalives. Unexpected and malformed
/// updates are errors: accepting them as arrivals would silently skew comparisons.
pub fn extract_event_key(
    update: &SubscribeUpdate,
    kind: StreamKind,
) -> Result<Option<EventKey>, &'static str> {
    match (&update.update_oneof, kind.data_type) {
        (Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_)), _) => Ok(None),
        (Some(UpdateOneof::Account(account)), DataType::Account) => {
            let info = account.account.as_ref().ok_or("missing account info")?;
            if info.pubkey.len() != 32 {
                return Err("invalid account pubkey");
            }
            let txn_signature = info
                .txn_signature
                .as_ref()
                .ok_or("missing account transaction signature")?;
            if txn_signature.len() != 64 {
                return Err("invalid account transaction signature");
            }
            Ok(Some(EventKey::Account {
                slot: account.slot,
                pubkey: info.pubkey.clone(),
                txn_signature: txn_signature.clone(),
            }))
        }
        (Some(UpdateOneof::Transaction(transaction)), DataType::Transaction) => {
            let signature = &transaction
                .transaction
                .as_ref()
                .ok_or("missing transaction info")?
                .signature;
            if signature.len() != 64 {
                return Err("invalid transaction signature");
            }
            Ok(Some(EventKey::Transaction {
                slot: transaction.slot,
                signature: signature.clone(),
            }))
        }
        (Some(UpdateOneof::Slot(slot)), DataType::Slot) => {
            if SlotStatus::try_from(slot.status).is_err() {
                return Err("invalid slot status");
            }
            Ok(Some(EventKey::Slot {
                slot: slot.slot,
                status: slot.status,
            }))
        }
        _ => Err("missing or unexpected update type"),
    }
}

/// Low-rate lifecycle and incomplete-period notifications. `at` is monotonic;
/// `utc` is only for historical reporting. Never put credentials in `reason`.
#[derive(Debug, Clone)]
pub enum StreamControl {
    Connected {
        provider: usize,
        stream: StreamKind,
        at: Instant,
        utc: SystemTime,
        reason: String,
    },
    Disconnected {
        provider: usize,
        stream: StreamKind,
        at: Instant,
        utc: SystemTime,
        reason: String,
    },
    Gap {
        provider: usize,
        stream: StreamKind,
        at: Instant,
        utc: SystemTime,
        reason: String,
    },
}

fn control_event(
    provider: usize,
    stream: StreamKind,
    state: &'static str,
    reason: &'static str,
    at: Instant,
) -> StreamControl {
    let utc = SystemTime::now();
    match state {
        "connected" => StreamControl::Connected {
            provider,
            stream,
            at,
            utc,
            reason: reason.into(),
        },
        "gap" => StreamControl::Gap {
            provider,
            stream,
            at,
            utc,
            reason: reason.into(),
        },
        _ => StreamControl::Disconnected {
            provider,
            stream,
            at,
            utc,
            reason: reason.into(),
        },
    }
}

/// Deliver a disconnect, waiting for room: no dropped lifecycle signal is
/// safe, and the reader is not reading updates meanwhile. While it waits, its
/// stamp stays published, so the matcher treats it as stalled (open comparisons
/// invalidated, health withheld) until the signal is queued. Returns false only
/// if the matcher has stopped.
async fn control(
    tx: &mpsc::Sender<StreamControl>,
    provider: usize,
    stream: StreamKind,
    state: &'static str,
    reason: &'static str,
    in_flight: &InFlight,
) -> bool {
    let stamp = in_flight.stamp(provider);
    let event = control_event(provider, stream, state, reason, stamp.at());
    let sent = tx.send(event).await.is_ok();
    drop(stamp);
    sent
}

/// The caller holds the update's published stamp until this gap is enqueued.
/// Preserve both original clocks even if decoding or enqueueing was delayed.
async fn send_gap(
    tx: &mpsc::Sender<StreamControl>,
    provider: usize,
    stream: StreamKind,
    at: Instant,
    utc: SystemTime,
    reason: &'static str,
) -> bool {
    tx.send(StreamControl::Gap {
        provider,
        stream,
        at,
        utc,
        reason: reason.into(),
    })
    .await
    .is_ok()
}

/// Announce a new subscription without waiting. Waiting would leave its updates
/// unread and stamped late, so the provider would look slow because of our own
/// backlog; on `Full` the caller drops the subscription and retries instead.
fn announce_connected(
    tx: &mpsc::Sender<StreamControl>,
    provider: usize,
    stream: StreamKind,
    in_flight: &InFlight,
) -> Result<(), mpsc::error::TrySendError<StreamControl>> {
    let stamp = in_flight.stamp(provider);
    tx.try_send(control_event(
        provider,
        stream,
        "connected",
        "subscribed",
        stamp.at(),
    ))
}

/// Why a provider stream ended. `subscribed_for` is `None` when the attempt
/// failed before a subscription was established.
struct Disconnect {
    reason: &'static str,
    subscribed_for: Option<Duration>,
}

impl Disconnect {
    fn before_subscribe(reason: &'static str) -> Option<Self> {
        Some(Self {
            reason,
            subscribed_for: None,
        })
    }
}

/// Run one provider/stream pair until the arrival or control receiver closes.
/// Spawn one task per provider and `StreamKind`; reconnects use bounded backoff
/// that resets after a stable subscription.
/// A full arrival queue produces a gap and a fresh subscription, never a silent drop.
/// Local backlogs back off; the reader stops only when the matcher has stopped.
pub async fn read_stream(
    provider_index: usize,
    provider: ProviderConfig,
    kind: StreamKind,
    filters: FiltersConfig,
    arrivals: mpsc::Sender<Arrival>,
    controls: mpsc::Sender<StreamControl>,
    in_flight: Arc<InFlight>,
) {
    let mut backoff = INITIAL_RETRY_DELAY;
    loop {
        if arrivals.is_closed() || controls.is_closed() {
            return;
        }
        tracing::debug!(
            provider = %provider.name,
            data_type = kind.data_type.as_str(),
            commitment = kind.commitment.as_str(),
            "connecting provider stream"
        );
        let Some(disconnect) = read_connection(
            provider_index,
            &provider,
            kind,
            &filters,
            &arrivals,
            &controls,
            &in_flight,
        )
        .await
        else {
            return;
        };
        let (delay, next_backoff) = retry_after(backoff, &disconnect);
        backoff = next_backoff;
        tracing::warn!(
            provider = %provider.name,
            data_type = kind.data_type.as_str(),
            commitment = kind.commitment.as_str(),
            reason = disconnect.reason,
            subscribed_seconds = disconnect.subscribed_for.map(|d| d.as_secs()),
            retry_in_seconds = delay.as_secs_f64(),
            "provider stream disconnected; retrying"
        );
        if !control(
            &controls,
            provider_index,
            kind,
            "disconnected",
            disconnect.reason,
            &in_flight,
        )
        .await
        {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {},
            _ = arrivals.closed() => return,
            _ = controls.closed() => return,
        }
    }
}

/// Returns a static lifecycle reason; diagnostics are logged separately.
async fn read_connection(
    provider_index: usize,
    provider: &ProviderConfig,
    kind: StreamKind,
    filters: &FiltersConfig,
    arrivals: &mpsc::Sender<Arrival>,
    controls: &mpsc::Sender<StreamControl>,
    in_flight: &InFlight,
) -> Option<Disconnect> {
    let token = match &provider.token_env {
        Some(name) => match std::env::var(name) {
            Ok(value) if !value.is_empty() => Some(value),
            _ => return Disconnect::before_subscribe("provider credential unavailable"),
        },
        None => None,
    };
    let builder = match GeyserGrpcClient::build_from_shared(provider.url.clone()) {
        Ok(builder) => builder,
        Err(error) => {
            return Disconnect::before_subscribe(connection_failure(
                provider,
                kind,
                "invalid provider endpoint",
                &error,
            ))
        }
    };
    let builder = match builder.x_token(token) {
        Ok(builder) => builder,
        Err(error) => {
            return Disconnect::before_subscribe(connection_failure(
                provider,
                kind,
                "invalid provider credential",
                &error,
            ))
        }
    };
    let builder = match builder.tls_config(ClientTlsConfig::new().with_native_roots()) {
        Ok(builder) => builder,
        Err(error) => {
            return Disconnect::before_subscribe(connection_failure(
                provider,
                kind,
                "TLS configuration failed",
                &error,
            ))
        }
    };
    let mut client = match builder
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
    {
        Ok(client) => client,
        Err(error) => {
            return Disconnect::before_subscribe(connection_failure(
                provider,
                kind,
                "connection failed",
                &error,
            ))
        }
    };
    let (mut sink, mut stream) = match tokio::time::timeout(
        SUBSCRIBE_TIMEOUT,
        client.subscribe_with_request(Some(subscribe_request(kind, filters))),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(error)) => {
            return Disconnect::before_subscribe(connection_failure(
                provider,
                kind,
                "subscription failed",
                &error,
            ))
        }
        Err(_) => return Disconnect::before_subscribe("subscription timed out"),
    };
    match announce_connected(controls, provider_index, kind, in_flight) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Closed(_)) => return None,
        Err(mpsc::error::TrySendError::Full(_)) => {
            return Disconnect::before_subscribe(CONTROL_QUEUE_FULL)
        }
    }
    tracing::info!(
        provider = %provider.name,
        data_type = kind.data_type.as_str(),
        commitment = kind.commitment.as_str(),
        "provider stream subscribed"
    );
    let subscribed_at = Instant::now();
    let ended = |reason| {
        Some(Disconnect {
            reason,
            subscribed_for: Some(subscribed_at.elapsed()),
        })
    };
    let mut last_message = subscribed_at;
    let mut ping =
        tokio::time::interval_at(subscribed_at + CLIENT_PING_INTERVAL, CLIENT_PING_INTERVAL);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping_id: i32 = 0;
    loop {
        // Updates take priority; pings and shutdown checks only run while idle.
        let next = tokio::select! {
            biased;
            next = stream.next() => next,
            _ = ping.tick() => {
                if last_message.elapsed() >= IDLE_TIMEOUT {
                    return ended("stream idle timeout");
                }
                ping_id = ping_id.wrapping_add(1);
                if sink
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: ping_id }),
                        ..Default::default()
                    })
                    .await
                    .is_err()
                {
                    return ended("ping send failed");
                }
                continue;
            }
            _ = arrivals.closed() => return None,
            _ = controls.closed() => return None,
        };
        // These timestamps must precede key extraction and queue operations.
        let stamp = in_flight.stamp(provider_index);
        let at = stamp.at();
        let utc = SystemTime::now();
        last_message = at;
        let update = match next {
            Some(Ok(update)) => update,
            Some(Err(error)) => {
                return ended(connection_failure(provider, kind, "stream error", &error))
            }
            None => return ended("stream EOF"),
        };
        if matches!(update.update_oneof, Some(UpdateOneof::Ping(_))) {
            drop(stamp); // A keepalive has no matched arrival; do not hold through I/O.
            if sink
                .send(SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                })
                .await
                .is_err()
            {
                return ended("pong send failed");
            }
            continue;
        }
        match extract_event_key(&update, kind) {
            Ok(Some(key)) => {
                match arrivals.try_send(Arrival {
                    provider: provider_index,
                    message_bytes: matches!(
                        kind.data_type,
                        DataType::Account | DataType::Transaction
                    )
                    .then(|| update.encoded_len() as u64),
                    key,
                    at,
                    utc,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => return None,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        if !send_gap(controls, provider_index, kind, at, utc, ARRIVAL_QUEUE_FULL)
                            .await
                        {
                            return None;
                        }
                        return ended(ARRIVAL_QUEUE_FULL);
                    }
                }
            }
            Ok(None) => {}
            Err(_) => {
                if !send_gap(
                    controls,
                    provider_index,
                    kind,
                    at,
                    utc,
                    "invalid stream update",
                )
                .await
                {
                    return None;
                }
                return ended("invalid stream update");
            }
        }
        drop(stamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_diagnostics_never_include_server_messages_or_credentials() {
        let secret = "https://provider.invalid/private-token?api-key=secret";
        let error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, secret);
        assert_eq!(
            connection_error_detail(&error),
            "I/O error: ConnectionRefused"
        );
        let error = std::io::Error::new(std::io::ErrorKind::TimedOut, secret);
        let wrapper = anyhow::Error::new(error).context("outer transport error");
        assert_eq!(
            connection_error_detail(wrapper.as_ref()),
            "I/O error: TimedOut"
        );
        for (code, message) in [
            (yellowstone_grpc_proto::tonic::Code::DataLoss, secret),
            (yellowstone_grpc_proto::tonic::Code::Internal, secret),
        ] {
            let status = yellowstone_grpc_proto::tonic::Status::new(code, message);
            let wrapper = anyhow::Error::new(status).context("stream failed");
            assert_eq!(
                connection_error_detail(wrapper.as_ref()),
                format!("gRPC status: {code:?}")
            );
        }
        let error = anyhow::anyhow!("unexpected failure: {secret}");
        assert_eq!(
            connection_error_detail(error.as_ref()),
            "unclassified failure (error text omitted to protect credentials)"
        );
    }

    use yellowstone_grpc_proto::prelude::{
        SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdatePing,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
    };

    fn kind(data_type: DataType, commitment: Commitment) -> StreamKind {
        StreamKind {
            data_type,
            commitment,
        }
    }
    fn update(oneof: UpdateOneof) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(oneof),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn full_control_channel_waits_for_room_without_dropping() {
        let kind = kind(DataType::Slot, Commitment::Processed);
        let (controls_tx, mut controls_rx) = mpsc::channel(1);
        let in_flight = Arc::new(InFlight::new(1));
        announce_connected(&controls_tx, 0, kind, &in_flight).unwrap();
        // A new subscription is never announced late: the reader retries instead.
        assert!(matches!(
            announce_connected(&controls_tx, 0, kind, &in_flight),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        // A gap waits for room, keeping its stamp published so the matcher
        // treats the reader as stalled meanwhile.
        let stamp = in_flight.stamp(0);
        let at = stamp.at();
        let utc = SystemTime::now();
        let gap = send_gap(&controls_tx, 0, kind, at, utc, ARRIVAL_QUEUE_FULL);
        tokio::pin!(gap);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut gap)
            .await
            .is_err());
        assert!(in_flight.published_before(Instant::now()));
        assert!(matches!(
            controls_rx.recv().await,
            Some(StreamControl::Connected { .. })
        ));
        assert!(gap.await);
        drop(stamp);
        assert!(!in_flight.published_before(Instant::now() + Duration::from_secs(1)));
        assert!(matches!(
            controls_rx.recv().await,
            Some(StreamControl::Gap { at: received_at, utc: received_utc, .. })
                if received_at == at && received_utc == utc
        ));
        // Only a stopped matcher stops the reader.
        drop(controls_rx);
        assert!(!control(&controls_tx, 0, kind, "disconnected", "x", &in_flight).await);
        assert!(matches!(
            announce_connected(&controls_tx, 0, kind, &in_flight),
            Err(mpsc::error::TrySendError::Closed(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_gap_invalidates_the_dropped_updates_original_window() {
        use crate::matcher::Matcher;
        let kind = kind(DataType::Transaction, Commitment::Finalized);
        let start = Instant::now();
        let utc = SystemTime::now();
        let mut matcher = Matcher::new(
            kind,
            2,
            Duration::from_secs(5),
            Duration::ZERO,
            100,
            Duration::from_secs(120),
            100,
        );
        for provider in 0..2 {
            matcher.connection_change(provider, start, utc, true, "subscribed");
        }
        matcher.arrival(Arrival {
            provider: 0,
            message_bytes: None,
            key: EventKey::Transaction {
                slot: 100,
                signature: vec![1; 64],
            },
            at: start,
            utc,
        });
        let in_flight = InFlight::new(2);
        tokio::time::advance(Duration::from_secs(1)).await;
        let stamp = in_flight.stamp(1);
        tokio::time::advance(Duration::from_secs(5)).await;
        let (tx, mut rx) = mpsc::channel(1);
        assert!(send_gap(&tx, 1, kind, stamp.at(), utc, ARRIVAL_QUEUE_FULL).await);
        drop(stamp);
        let StreamControl::Gap {
            at, utc, reason, ..
        } = rx.recv().await.unwrap()
        else {
            panic!("gap expected")
        };
        matcher.gap(Some(1), at, utc, reason);
        assert!(!in_flight.published_before(Instant::now()));
        assert!(
            matcher.close(Instant::now()).is_empty(),
            "lost on-time input must not produce a drop or win"
        );
    }

    #[test]
    fn local_backlog_disconnects_always_back_off() {
        let long = Some(Duration::from_secs(3600));
        for reason in LOCAL_DISCONNECTS {
            let disconnect = Disconnect {
                reason,
                subscribed_for: long,
            };
            assert_eq!(
                retry_after(INITIAL_RETRY_DELAY, &disconnect),
                (INITIAL_RETRY_DELAY, INITIAL_RETRY_DELAY * 2)
            );
        }
        // A provider's disconnect after a stable subscription still reconnects at once.
        let provider = Disconnect {
            reason: "stream EOF",
            subscribed_for: long,
        };
        assert_eq!(
            retry_after(INITIAL_RETRY_DELAY * 8, &provider),
            (Duration::ZERO, INITIAL_RETRY_DELAY)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stamp_publication_reports_only_old_unqueued_stamps() {
        let in_flight = InFlight::new(2);
        let outer = in_flight.stamp(1);
        let stamped = outer.at();
        assert!(!in_flight.published_before(stamped));
        assert!(in_flight.published_before(stamped + Duration::from_nanos(1)));
        tokio::time::advance(Duration::from_secs(5)).await;
        {
            // A nested control stamp must not hide or clear the older update stamp.
            let nested = in_flight.stamp(1);
            assert!(nested.at() > stamped);
        }
        assert!(in_flight.published_before(stamped + Duration::from_nanos(1)));
        drop(outer);
        assert!(!in_flight.published_before(Instant::now() + Duration::from_secs(1)));
        let other = in_flight.stamp(0);
        assert!(!in_flight.published_before(other.at()));
    }

    #[test]
    fn dashboards_exclude_local_backlog_from_provider_downtime() {
        // alerts.ts imports the same list.
        let local = format!("('{}')", LOCAL_DISCONNECTS.join("', '"));
        assert!(
            include_str!("../grafana/queries.ts").contains(&local),
            "{local}"
        );
    }

    #[test]
    fn stable_subscriptions_reconnect_immediately_and_failures_back_off() {
        let stable = Some(STABLE_SUBSCRIPTION);
        assert_eq!(
            retry_delay(MAX_RETRY_DELAY, stable),
            (Duration::ZERO, INITIAL_RETRY_DELAY)
        );
        let mut backoff = INITIAL_RETRY_DELAY;
        let mut delays = Vec::new();
        for subscribed_for in [
            None,
            Some(Duration::from_secs(5)),
            None,
            None,
            None,
            None,
            None,
            None,
        ] {
            let (delay, next_backoff) = retry_delay(backoff, subscribed_for);
            backoff = next_backoff;
            delays.push(delay.as_secs());
        }
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(
            retry_delay(backoff, Some(Duration::from_secs(3600))),
            (Duration::ZERO, INITIAL_RETRY_DELAY)
        );
    }

    #[test]
    fn request_is_one_filtered_stream_for_each_active_kind() {
        let filters = FiltersConfig {
            accounts_owner: vec!["owner-b".into(), "owner-a".into(), "owner-b".into()],
            transactions_account_include: vec!["account-b".into(), "account-a".into()],
            ..Default::default()
        };
        for active in StreamKind::ALL {
            let StreamKind {
                data_type,
                commitment,
            } = active;
            let request = subscribe_request(active, &filters);
            assert_eq!(
                request.commitment,
                Some(match commitment {
                    Commitment::Processed => 0,
                    Commitment::Finalized => 2,
                })
            );
            assert_eq!(
                request.accounts.len() + request.transactions.len() + request.slots.len(),
                1
            );
            assert!(request.from_slot.is_none() && request.accounts_data_slice.is_empty());
            assert!(
                request.transactions_status.is_empty()
                    && request.blocks.is_empty()
                    && request.blocks_meta.is_empty()
                    && request.entry.is_empty()
                    && request.ping.is_none()
            );
            match data_type {
                DataType::Account => assert_eq!(
                    request.accounts[FILTER_KEY],
                    SubscribeRequestFilterAccounts {
                        owner: vec!["owner-a".into(), "owner-b".into()],
                        nonempty_txn_signature: Some(true),
                        ..Default::default()
                    }
                ),
                DataType::Transaction => assert_eq!(
                    request.transactions[FILTER_KEY],
                    SubscribeRequestFilterTransactions {
                        account_include: vec!["account-a".into(), "account-b".into()],
                        ..Default::default()
                    }
                ),
                DataType::Slot => {
                    assert_eq!(request.slots[FILTER_KEY].filter_by_commitment, Some(false));
                    assert_eq!(request.slots[FILTER_KEY].interslot_updates, Some(true));
                }
            }
        }
    }

    #[test]
    fn empty_lists_request_transaction_linked_accounts_and_all_transactions() {
        let filters = FiltersConfig {
            allow_all_accounts: true,
            allow_all_transactions: true,
            ..Default::default()
        };
        assert_eq!(
            subscribe_request(kind(DataType::Account, Commitment::Processed), &filters).accounts
                [FILTER_KEY],
            SubscribeRequestFilterAccounts {
                nonempty_txn_signature: Some(true),
                ..Default::default()
            }
        );
        assert_eq!(
            subscribe_request(kind(DataType::Transaction, Commitment::Finalized), &filters)
                .transactions[FILTER_KEY],
            SubscribeRequestFilterTransactions::default()
        );
    }

    #[test]
    fn account_keys_require_signatures_and_ignore_node_local_write_versions() {
        let account = SubscribeUpdateAccount {
            slot: 42,
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: vec![7; 32],
                txn_signature: Some(vec![8; 64]),
                write_version: 100,
                ..Default::default()
            }),
            ..Default::default()
        };
        for commitment in Commitment::ALL {
            let kind = kind(DataType::Account, commitment);
            let extract = |account| extract_event_key(&update(UpdateOneof::Account(account)), kind);
            let key = extract(account.clone()).unwrap().unwrap();
            let mut another_node = account.clone();
            another_node.account.as_mut().unwrap().write_version = 999_999;
            assert_eq!(extract(another_node).unwrap().unwrap(), key);
            let mut another_transaction = account.clone();
            another_transaction.account.as_mut().unwrap().txn_signature = Some(vec![9; 64]);
            assert_ne!(extract(another_transaction).unwrap().unwrap(), key);
            for signature in [None, Some(vec![]), Some(vec![8; 63]), Some(vec![8; 65])] {
                let mut invalid = account.clone();
                invalid.account.as_mut().unwrap().txn_signature = signature;
                assert!(
                    extract(invalid).is_err(),
                    "unidentifiable updates must not open comparisons"
                );
            }
        }
    }

    #[test]
    fn extracts_keys_and_rejects_malformed_or_wrong_types() {
        let account = update(UpdateOneof::Account(SubscribeUpdateAccount {
            slot: 42,
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: vec![7; 32],
                txn_signature: Some(vec![9; 64]),
                ..Default::default()
            }),
            ..Default::default()
        }));
        let transaction = update(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot: 43,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![8; 64],
                ..Default::default()
            }),
            ..Default::default()
        }));
        let slot = update(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 44,
            status: SlotStatus::SlotFinalized as i32,
            ..Default::default()
        }));
        let a = kind(DataType::Account, Commitment::Processed);
        let t = kind(DataType::Transaction, Commitment::Processed);
        let s = kind(DataType::Slot, Commitment::Finalized);
        assert_eq!(
            extract_event_key(&account, a).unwrap(),
            Some(EventKey::Account {
                slot: 42,
                pubkey: vec![7; 32],
                txn_signature: vec![9; 64],
            })
        );
        assert_eq!(
            extract_event_key(&transaction, t).unwrap(),
            Some(EventKey::Transaction {
                slot: 43,
                signature: vec![8; 64]
            })
        );
        assert_eq!(
            extract_event_key(&slot, s).unwrap(),
            Some(EventKey::Slot {
                slot: 44,
                status: 2
            })
        );
        assert!(extract_event_key(&account, t).is_err());
        assert!(extract_event_key(&SubscribeUpdate::default(), a).is_err());
        assert!(extract_event_key(
            &update(UpdateOneof::Account(SubscribeUpdateAccount::default())),
            a
        )
        .is_err());
        assert!(extract_event_key(
            &update(UpdateOneof::Transaction(
                SubscribeUpdateTransaction::default()
            )),
            t
        )
        .is_err());
        assert!(extract_event_key(
            &update(UpdateOneof::Slot(SubscribeUpdateSlot {
                status: 999,
                ..Default::default()
            })),
            s
        )
        .is_err());
        assert_eq!(
            extract_event_key(&update(UpdateOneof::Ping(SubscribeUpdatePing {})), a).unwrap(),
            None
        );
    }
}
