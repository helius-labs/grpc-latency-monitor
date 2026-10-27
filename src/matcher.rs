//! Bounded, synchronous matching of arrivals from one subscription stream.
//! Drive all methods on one task, in timestamp order for connection/gap changes;
//! call `close` after the integration layer's processing grace has elapsed.

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

use crate::model::{
    Arrival, ConnectionRecord, EventKey, GapRecord, LagRecord, Outcome, StorageRecord, StreamKind,
};

struct ProviderState {
    /// When the provider became eligible for misses: connection plus warmup.
    connected_since: Option<Instant>,
    last_gap: Option<Instant>,
}

struct Observation {
    // Also suppresses duplicate deliveries while the window is open.
    arrived: Option<Instant>,
    message_bytes: Option<u64>,
    eligible: bool,
}

struct Event {
    first: Instant,
    first_utc: SystemTime,
    deadline: Instant,
    // Keep provider state together: one allocation per key, including deduplication.
    observations: Vec<Observation>,
    // An input gap can hide an earlier first arrival, invalidating every lag
    // classification for this key, not merely the missing-provider result.
    incomplete: bool,
}

/// Closed or rejected keys, remembered as 64-bit digests so a late or repeated
/// delivery cannot start a new comparison window. Without this, a provider
/// arriving after the window would become the "first" arrival (lag 0) and every
/// provider that delivered on time would be recorded as missing the deadline.
/// Memory is bounded: the oldest entries are evicted beyond `capacity`. A digest
/// forgotten for any reason raises a slot floor instead, and keys at or
/// below the floor are treated as remembered: they are at least as old as the
/// eviction horizon, so they cannot be a trustworthy first arrival.
struct ExpiredKeys {
    retention: Duration,
    capacity: usize,
    forget_at: HashMap<u64, Instant>,
    order: VecDeque<(Instant, u64, SlotFloor)>,
    /// Highest slot forgotten, per floor class. Survives normal digest expiry.
    floors: HashMap<FloorClass, u64>,
}

/// Slot statuses of one slot legitimately arrive seconds apart (finalized
/// trails processed), so each status has its own floor. Accounts and
/// transactions share one.
type FloorClass = Option<i32>;
type SlotFloor = (FloorClass, u64);

fn slot_floor(key: &EventKey) -> SlotFloor {
    match key {
        EventKey::Slot { slot, status } => (Some(*status), *slot),
        _ => (None, key.slot()),
    }
}

impl ExpiredKeys {
    fn contains(&self, digest: u64, (class, slot): SlotFloor) -> bool {
        self.forget_at.contains_key(&digest)
            || self.floors.get(&class).is_some_and(|floor| slot <= *floor)
    }

    fn insert(&mut self, digest: u64, floor: SlotFloor, until: Instant) {
        while self.order.len() >= self.capacity {
            self.pop_oldest();
        }
        let forget_at = self.forget_at.entry(digest).or_insert(until);
        *forget_at = (*forget_at).max(until);
        self.order.push_back((until, digest, floor));
    }

    fn prune(&mut self, now: Instant) {
        while self.order.front().is_some_and(|(until, ..)| *until < now) {
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        if let Some((until, digest, (class, slot))) = self.order.pop_front() {
            // A later insertion of the same digest extends its retention.
            if self.forget_at.get(&digest).is_some_and(|at| *at <= until) {
                self.forget_at.remove(&digest);
                // Normal expiry must not allow an old replay to become first again.
                let floor = self.floors.entry(class).or_insert(slot);
                *floor = (*floor).max(slot);
            }
        }
    }
}

/// Stable within the process; collisions only suppress an unrelated new key.
fn digest(key: &EventKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Matches one `StreamKind` across `provider_count` provider indices (0..count).
///
/// `window` is half-open and starts at the first provider's delivery: arrivals
/// strictly before the deadline are on time. Anything later misses the deadline:
/// an eligible provider without an on-time arrival is recorded as missing the deadline, even
/// if its delivery arrives afterwards. There is no separate late outcome.
///
/// When its window closes, a key is classified and removed; its digest is then
/// remembered for `expired_key_retention` so later deliveries of the key are
/// ignored instead of starting a new window. The digests are bounded by
/// `expired_key_capacity` entries; beyond that, the oldest are replaced by a
/// per-stream slot floor, so a key at or below the highest slot forgotten
/// is ignored too. Closed keys cannot be admitted again in this process.
/// `max_tracked_keys` caps open windows. If the cap is reached, a new key is
/// discarded and remembered like a closed key, so a later delivery cannot take
/// its place; one gap per `close` interval marks the incomplete period.
/// Unrelated open windows are unaffected.
///
/// The caller must send connection/gap signals before closing their affected
/// windows, and must process arrivals before calling `close` past their stamp.
/// Arrival stamps within an open event may be out of order: the minimum stamp
/// (and its UTC) determines the first arrival. Once closed, results are final.
/// Connection state starts unknown and does not qualify for misses.
/// All methods return records to enqueue for storage; none performs I/O.
pub struct Matcher {
    stream: StreamKind,
    window: Duration,
    /// A provider qualifies for misses only this long after subscribing. A
    /// subscription is accepted before the server applies its filter, and events
    /// from just before it may still be delivered first by a slower provider; the
    /// new subscriber can never send those, so counting them would be false missed deadlines.
    connection_warmup: Duration,
    max_tracked_keys: usize,
    providers: Vec<ProviderState>,
    global_gap: Option<Instant>,
    events: HashMap<EventKey, Event>,
    expired: ExpiredKeys,
    /// Set when a key is rejected at capacity; cleared by `close`. Coalesces
    /// overload records so rejections stay O(1) under sustained overload.
    overloaded: bool,
}

impl Matcher {
    pub fn new(
        stream: StreamKind,
        provider_count: usize,
        window: Duration,
        connection_warmup: Duration,
        max_tracked_keys: usize,
        expired_key_retention: Duration,
        expired_key_capacity: usize,
    ) -> Self {
        assert!(provider_count > 0, "at least one provider is required");
        assert!(max_tracked_keys > 0, "at least one tracked key is required");
        assert!(
            expired_key_capacity > 0,
            "at least one expired key is required"
        );
        assert!(!window.is_zero(), "window must be nonzero");
        Self {
            stream,
            window,
            connection_warmup,
            max_tracked_keys,
            providers: (0..provider_count)
                .map(|_| ProviderState {
                    connected_since: None,
                    last_gap: None,
                })
                .collect(),
            global_gap: None,
            events: HashMap::new(),
            expired: ExpiredKeys {
                retention: expired_key_retention,
                capacity: expired_key_capacity,
                forget_at: HashMap::new(),
                order: VecDeque::new(),
                floors: HashMap::new(),
            },
            overloaded: false,
        }
    }

    /// Consume an observation; duplicate deliveries from the same provider
    /// and key (including repeated account writes) are ignored.
    pub fn arrival(&mut self, arrival: Arrival) -> Vec<StorageRecord> {
        self.observe(arrival, false)
    }

    /// A valid comparison can finish while its slot ages below admission bounds.
    /// This never permits opening a new window or replaying a closed comparison.
    pub(crate) fn has_open(&self, key: &EventKey) -> bool {
        self.events.contains_key(key)
    }

    /// A stamp delayed beyond processing grace cannot establish a trustworthy
    /// first arrival, even when it is the first input seen for a new key.
    pub fn arrival_incomplete(&mut self, arrival: Arrival) -> Vec<StorageRecord> {
        self.observe(arrival, true)
    }

    fn observe(&mut self, arrival: Arrival, incomplete: bool) -> Vec<StorageRecord> {
        assert!(
            arrival.provider < self.providers.len(),
            "invalid provider index"
        );
        let mut records = Vec::new();
        let Arrival {
            provider,
            key,
            at,
            utc,
            message_bytes,
        } = arrival;
        if !self.events.contains_key(&key) {
            let digest = digest(&key);
            let floor = slot_floor(&key);
            if self.expired.contains(digest, floor) {
                // After its window (or a repeat): already classified as
                // a missed deadline for this provider. Never start a new first arrival.
                return records;
            }
            if self.events.len() == self.max_tracked_keys {
                // Remember the dropped key: a later delivery of it must not
                // become the first arrival. Other keys' windows are unaffected.
                let until = at + self.window + self.expired.retention;
                self.expired.insert(digest, floor, until);
                if !self.overloaded {
                    self.overloaded = true;
                    records.push(StorageRecord::Gap(GapRecord {
                        stream: self.stream,
                        provider: None,
                        utc,
                        reason: "matcher key capacity exceeded".into(),
                    }));
                }
                return records;
            }
            // Only the latest gap is retained; it may hide an older gap
            // inside a backdated event's window.
            let overlaps_gap = self.global_gap.is_some_and(|gap| at <= gap)
                || self
                    .providers
                    .iter()
                    .any(|state| state.last_gap.is_some_and(|gap| at <= gap));
            let observations = self
                .providers
                .iter()
                .map(|state| Observation {
                    arrived: None,
                    message_bytes: None,
                    eligible: state.connected_since.is_some_and(|since| since <= at)
                    // Conservative for backdated arrivals: a newer gap can
                    // hide an earlier gap after its timestamp was overwritten.
                    && !state.last_gap.is_some_and(|gap| at <= gap)
                    && !self.global_gap.is_some_and(|gap| at <= gap),
                })
                .collect();
            self.events.insert(
                key.clone(),
                Event {
                    first: at,
                    first_utc: utc,
                    deadline: at + self.window,
                    observations,
                    incomplete: incomplete || overlaps_gap,
                },
            );
        }
        let event = self.events.get_mut(&key).expect("key inserted");
        event.incomplete |= incomplete;
        if event.observations[provider].arrived.is_some() {
            return records;
        }
        if at < event.first {
            event.first = at;
            event.first_utc = utc;
            event.deadline = at + self.window;
            event.incomplete |= self.global_gap.is_some_and(|gap| at <= gap)
                || self
                    .providers
                    .iter()
                    .any(|state| state.last_gap.is_some_and(|gap| at <= gap));
            for (state, observation) in self.providers.iter().zip(&mut event.observations) {
                observation.eligible &= state.connected_since.is_some_and(|since| since <= at)
                    && !state.last_gap.is_some_and(|gap| at <= gap)
                    && !self.global_gap.is_some_and(|gap| at <= gap);
            }
        }
        // Even if the stamp is beyond the deadline, wait until the caller
        // closes the window: a reordered earlier observation can still move
        // the first arrival and its deadline.
        event.observations[provider].arrived = Some(at);
        event.observations[provider].message_bytes = message_bytes;
        records
    }

    /// Report a connected/disconnected transition. A reconnect never repairs
    /// eligibility for windows that started while disconnected, and a provider
    /// qualifies for misses only after the connection warmup.
    pub fn connection_change(
        &mut self,
        provider: usize,
        at: Instant,
        utc: SystemTime,
        connected: bool,
        reason: impl Into<String>,
    ) -> Vec<StorageRecord> {
        let state = self
            .providers
            .get_mut(provider)
            .expect("invalid provider index");
        if connected {
            if state.connected_since.is_none() {
                state.connected_since = Some(at + self.connection_warmup);
            }
        } else {
            state.connected_since = None;
            // Also events that start after the disconnect: another reader's later
            // arrival may have opened one before this control was processed.
            for event in self.events.values_mut() {
                if at < event.deadline {
                    event.observations[provider].eligible = false;
                }
            }
        }
        vec![StorageRecord::Connection(ConnectionRecord {
            stream: self.stream,
            provider,
            utc,
            connected,
            reason: reason.into(),
        })]
    }

    /// When a connected provider becomes eligible for misses, after warmup.
    pub fn eligible_since(&self, provider: usize) -> Option<Instant> {
        self.providers[provider].connected_since
    }

    /// Mark incomplete data for one provider or all providers. A gap inside an
    /// open event's window invalidates every comparison: a dropped input may
    /// have been the true first arrival, even on another provider.
    pub fn gap(
        &mut self,
        provider: Option<usize>,
        at: Instant,
        utc: SystemTime,
        reason: impl Into<String>,
    ) -> Vec<StorageRecord> {
        // Never move a gap backwards: a stale (older) stamp must not hide a
        // newer gap from events created later.
        let latest = |gap: Option<Instant>| Some(gap.map_or(at, |gap| gap.max(at)));
        match provider {
            Some(index) => {
                let state = self
                    .providers
                    .get_mut(index)
                    .expect("invalid provider index");
                state.last_gap = latest(state.last_gap);
            }
            None => self.global_gap = latest(self.global_gap),
        }
        // Including events that start after the gap: the dropped input may have
        // been their true first arrival, processed after a later one opened them.
        for event in self.events.values_mut() {
            if at < event.deadline {
                event.incomplete = true;
                match provider {
                    Some(index) => event.observations[index].eligible = false,
                    None => event
                        .observations
                        .iter_mut()
                        .for_each(|observation| observation.eligible = false),
                }
            }
        }
        vec![StorageRecord::Gap(GapRecord {
            stream: self.stream,
            provider,
            utc,
            reason: reason.into(),
        })]
    }

    /// A delayed or backlogged input may hide an arrival/control whose timestamp
    /// predates `close`. Invalidate all open comparisons before closing while
    /// still closing keys on schedule; neither wins nor misses are trustworthy.
    pub fn invalidate_open(
        &mut self,
        provider: Option<usize>,
        at: Instant,
        utc: SystemTime,
        reason: impl Into<String>,
    ) -> Vec<StorageRecord> {
        let rows = self.gap(provider, at, utc, reason);
        for event in self.events.values_mut() {
            event.incomplete = true;
            match provider {
                Some(index) => event.observations[index].eligible = false,
                None => event
                    .observations
                    .iter_mut()
                    .for_each(|observation| observation.eligible = false),
            }
        }
        rows
    }

    /// Classify and remove windows whose deadline has passed, remembering their
    /// keys. Call periodically even when no arrivals are received.
    pub fn close(&mut self, now: Instant) -> Vec<StorageRecord> {
        self.overloaded = false;
        self.expired.prune(now);
        let mut records = Vec::new();
        let stream = self.stream;
        let expired = &mut self.expired;
        self.events.retain(|key, event| {
            if event.deadline > now {
                return true;
            }
            if !event.incomplete {
                for (provider, observation) in event.observations.iter().enumerate() {
                    // A stamp at or after the deadline misses the deadline by definition;
                    // an earlier out-of-order stamp can move the deadline before it.
                    let on_time = observation.arrived.filter(|stamp| *stamp < event.deadline);
                    if on_time.is_some() || observation.eligible {
                        records.push(StorageRecord::Lag(LagRecord {
                            stream,
                            key: key.clone(),
                            first_utc: event.first_utc,
                            provider,
                            outcome: if on_time.is_some() {
                                Outcome::OnTime
                            } else {
                                Outcome::Missed
                            },
                            message_bytes: on_time.and(observation.message_bytes),
                            lag_ns: on_time.map(|stamp| lag_ns(event.first, stamp)),
                        }));
                    }
                }
            }
            expired.insert(digest(key), slot_floor(key), now + expired.retention);
            false
        });
        records
    }
}

fn lag_ns(first: Instant, at: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(first).as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Commitment, DataType};

    fn stream() -> StreamKind {
        StreamKind {
            data_type: DataType::Account,
            commitment: Commitment::Processed,
        }
    }
    fn key(n: u64) -> EventKey {
        EventKey::Account {
            slot: n,
            pubkey: vec![1],
        }
    }
    fn arrival(provider: usize, n: u64, at: Instant) -> Arrival {
        Arrival {
            message_bytes: Some(100),
            provider,
            key: key(n),
            at,
            utc: SystemTime::UNIX_EPOCH,
        }
    }
    fn lags(rows: Vec<StorageRecord>) -> Vec<(usize, Outcome, Option<u64>)> {
        let mut rows: Vec<_> = rows
            .into_iter()
            .filter_map(|row| match row {
                StorageRecord::Lag(r) => Some((r.provider, r.outcome, r.lag_ns)),
                _ => None,
            })
            .collect();
        rows.sort_by_key(|r| r.0);
        rows
    }
    fn matcher(count: usize, cap: usize) -> Matcher {
        Matcher::new(
            stream(),
            count,
            Duration::from_secs(1),
            CONNECTION_WARMUP,
            cap,
            RETENTION,
            cap * 2,
        )
    }
    const RETENTION: Duration = Duration::from_secs(60);
    const CONNECTION_WARMUP: Duration = Duration::from_secs(1);
    /// Connect so that every provider's warmup has ended by `t`.
    fn connect_all(m: &mut Matcher, count: usize, t: Instant) {
        for i in 0..count {
            m.connection_change(
                i,
                t - CONNECTION_WARMUP,
                SystemTime::UNIX_EPOCH,
                true,
                "initial",
            );
        }
    }

    #[test]
    fn message_sizes_keep_each_providers_first_delivery_and_misses_are_null() {
        for data_type in [
            crate::model::DataType::Account,
            crate::model::DataType::Transaction,
        ] {
            let mut m = Matcher::new(
                StreamKind {
                    data_type,
                    ..stream()
                },
                3,
                Duration::from_secs(1),
                CONNECTION_WARMUP,
                10,
                RETENTION,
                20,
            );
            let t = Instant::now();
            connect_all(&mut m, 3, t);
            let event_key = match data_type {
                crate::model::DataType::Account => key(1),
                _ => EventKey::Transaction {
                    slot: 1,
                    signature: vec![1; 64],
                },
            };
            for (provider, bytes) in [(0, 123), (0, 999), (1, 456)] {
                m.arrival(Arrival {
                    provider,
                    key: event_key.clone(),
                    at: t,
                    utc: SystemTime::UNIX_EPOCH,
                    message_bytes: Some(bytes),
                });
            }
            let rows = m.close(t + Duration::from_secs(1));
            let sizes: Vec<_> = rows
                .into_iter()
                .filter_map(|r| match r {
                    StorageRecord::Lag(r) => Some((r.provider, r.outcome, r.message_bytes)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                sizes,
                vec![
                    (0, Outcome::OnTime, Some(123)),
                    (1, Outcome::OnTime, Some(456)),
                    (2, Outcome::Missed, None)
                ]
            );
            // A delivery after the window misses the deadline: no row, no size.
            assert!(m
                .arrival(Arrival {
                    provider: 2,
                    key: event_key,
                    at: t + Duration::from_secs(2),
                    utc: SystemTime::UNIX_EPOCH,
                    message_bytes: Some(789),
                })
                .is_empty());
            assert!(m.close(t + Duration::from_secs(3)).is_empty());
        }
    }

    #[test]
    fn ties_duplicates_and_earlier_out_of_order_stamp() {
        let t = Instant::now();
        let mut m = matcher(3, 2);
        connect_all(&mut m, 3, t);
        m.arrival(arrival(0, 1, t + Duration::from_millis(20)));
        m.arrival(arrival(1, 1, t));
        m.arrival(arrival(2, 1, t));
        m.arrival(arrival(0, 1, t + Duration::from_millis(10))); // duplicate
        assert_eq!(
            lags(m.close(t + Duration::from_secs(1))),
            vec![
                (0, Outcome::OnTime, Some(20_000_000)),
                (1, Outcome::OnTime, Some(0)),
                (2, Outcome::OnTime, Some(0)),
            ]
        );
    }

    #[test]
    fn deadline_is_exclusive_and_later_deliveries_are_dropped() {
        let t = Instant::now();
        let mut m = matcher(2, 2);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        // Exactly at the deadline is already too late.
        assert!(m
            .arrival(arrival(1, 1, t + Duration::from_secs(1)))
            .is_empty());
        assert_eq!(
            lags(m.close(t + Duration::from_secs(1))),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None)]
        );
        // After close, deliveries and duplicates of the key produce nothing.
        for provider in 0..2 {
            assert!(m
                .arrival(arrival(provider, 1, t + Duration::from_secs(2)))
                .is_empty());
        }
        assert!(m.close(t + Duration::from_secs(3)).is_empty());
        assert!(m.events.is_empty());
    }

    #[test]
    fn earlier_stamp_moves_deadline_and_drops_a_later_delivery() {
        let t = Instant::now();
        let mut m = matcher(2, 2);
        connect_all(&mut m, 2, t);
        // Processed out of order: B's earlier stamp becomes the first arrival,
        // which puts A's stamp beyond the deadline.
        m.arrival(arrival(0, 1, t + Duration::from_secs(2)));
        m.arrival(arrival(1, 1, t));
        assert_eq!(
            lags(m.close(t + Duration::from_secs(1))),
            vec![(0, Outcome::Missed, None), (1, Outcome::OnTime, Some(0))]
        );
    }

    #[test]
    fn disconnected_unknown_and_reconnected_are_not_missed() {
        let t = Instant::now();
        let mut m = matcher(3, 3);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        m.connection_change(
            1,
            t + Duration::from_millis(500),
            SystemTime::UNIX_EPOCH,
            false,
            "lost",
        );
        m.connection_change(
            1,
            t + Duration::from_millis(600),
            SystemTime::UNIX_EPOCH,
            true,
            "retry",
        );
        assert_eq!(
            lags(m.close(t + Duration::from_secs(1))),
            vec![(0, Outcome::OnTime, Some(0))]
        );
        m.arrival(arrival(0, 2, t + Duration::from_secs(2)));
        assert_eq!(
            lags(m.close(t + Duration::from_secs(3))),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None),]
        );
    }

    #[test]
    fn provider_is_not_missed_during_connection_warmup() {
        let t = Instant::now();
        let mut m = matcher(2, 3);
        connect_all(&mut m, 1, t);
        m.connection_change(1, t, SystemTime::UNIX_EPOCH, true, "subscribed");
        assert_eq!(m.eligible_since(1), Some(t + CONNECTION_WARMUP));
        // Inside warmup: a delivery still counts, a non-delivery is unknown.
        m.arrival(arrival(0, 1, t + CONNECTION_WARMUP / 2));
        m.arrival(arrival(1, 2, t + CONNECTION_WARMUP / 2));
        m.arrival(arrival(0, 2, t + CONNECTION_WARMUP / 2));
        assert_eq!(
            lags(m.close(t + CONNECTION_WARMUP * 2)),
            vec![
                (0, Outcome::OnTime, Some(0)),
                (0, Outcome::OnTime, Some(0)),
                (1, Outcome::OnTime, Some(0)),
            ]
        );
        // From the end of warmup on, the provider qualifies for misses.
        m.arrival(arrival(0, 3, t + CONNECTION_WARMUP));
        assert_eq!(
            lags(m.close(t + CONNECTION_WARMUP * 3)),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None)]
        );
    }

    #[test]
    fn provider_and_global_gaps_suppress_all_uncertain_comparisons() {
        let t = Instant::now();
        let mut m = matcher(3, 3);
        connect_all(&mut m, 3, t);
        m.arrival(arrival(0, 1, t));
        m.gap(
            Some(1),
            t + Duration::from_millis(100),
            SystemTime::UNIX_EPOCH,
            "drop",
        );
        assert!(lags(m.close(t + Duration::from_secs(1))).is_empty());
        m.arrival(arrival(0, 2, t + Duration::from_secs(2)));
        m.gap(
            None,
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
            "drop",
        );
        assert!(lags(m.close(t + Duration::from_secs(3))).is_empty());
    }

    #[test]
    fn backdated_event_after_gaps_cannot_invent_a_miss() {
        let t = Instant::now();
        let mut m = matcher(2, 2);
        connect_all(&mut m, 2, t);
        m.gap(
            Some(1),
            t + Duration::from_millis(100),
            SystemTime::UNIX_EPOCH,
            "first",
        );
        m.gap(
            Some(1),
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
            "second",
        );
        m.arrival(arrival(0, 1, t));
        assert!(lags(m.close(t + Duration::from_secs(1))).is_empty());
    }

    #[test]
    fn stale_new_key_and_delayed_peer_cannot_create_a_false_miss() {
        let t = Instant::now();
        let mut m = matcher(2, 2);
        connect_all(&mut m, 2, t);
        m.invalidate_open(None, t, SystemTime::UNIX_EPOCH, "processing grace exceeded");
        m.arrival_incomplete(arrival(0, 1, t));
        // Closing before B's earlier-in-window arrival may not classify either
        // provider; the queued stamp can be processed only after close.
        assert!(lags(m.close(t + Duration::from_secs(1))).is_empty());
        assert!(lags(m.arrival(arrival(1, 1, t + Duration::from_millis(500)))).is_empty());
        assert!(lags(m.close(t + Duration::from_secs(3))).is_empty());
        m.arrival(arrival(0, 2, t + Duration::from_secs(4)));
        assert_eq!(
            lags(m.close(t + Duration::from_secs(5))),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None)]
        );
    }

    #[test]
    fn backlog_discards_wrong_winner_even_if_earlier_stamp_arrives_after_close() {
        let t = Instant::now();
        let mut m = matcher(2, 1);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t + Duration::from_secs(1)));
        m.invalidate_open(
            None,
            t + Duration::from_secs(3),
            SystemTime::UNIX_EPOCH,
            "matcher or storage backlog",
        );
        assert!(lags(m.close(t + Duration::from_secs(3))).is_empty());
        // This stamp should have moved the first arrival from t+1s to t;
        // the existing A stamp would be late, not an on-time winner.
        assert!(lags(m.arrival(arrival(1, 1, t))).is_empty());
        assert!(lags(m.close(t + Duration::from_secs(4))).is_empty());
        m.close(t + Duration::from_secs(4) + Duration::from_nanos(1));
        assert!(m.events.is_empty());
        m.arrival(arrival(1, 2, t + Duration::from_secs(5)));
        assert_eq!(
            lags(m.close(t + Duration::from_secs(6))),
            vec![(0, Outcome::Missed, None), (1, Outcome::OnTime, Some(0))]
        );
    }

    #[test]
    fn full_capacity_drops_new_key_without_inventing_misses() {
        let t = Instant::now();
        let mut m = matcher(2, 1);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        let records = m.arrival(arrival(0, 2, t + Duration::from_millis(100)));
        assert!(matches!(
            &records[..],
            [StorageRecord::Gap(GapRecord { provider: None, .. })]
        ));
        // The tracked key is unrelated to the dropped one and is still classified.
        assert_eq!(
            lags(m.close(t + Duration::from_secs(1))),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None)]
        );
        assert!(m.events.is_empty());
        // Capacity is free again, but the dropped key's first arrival is unknown:
        // B's later delivery must not become the first arrival with A missed.
        assert!(m
            .arrival(arrival(1, 2, t + Duration::from_millis(4100)))
            .is_empty());
        assert!(lags(m.close(t + Duration::from_secs(6))).is_empty());
        assert!(m.events.is_empty());
    }

    #[test]
    fn capacity_rejections_record_one_gap_per_close_interval() {
        let t = Instant::now();
        let mut m = matcher(2, 1);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        let gaps = |rows: Vec<StorageRecord>| {
            rows.iter()
                .filter(|row| matches!(row, StorageRecord::Gap(_)))
                .count()
        };
        let rejected: usize = (2..1_000)
            .map(|n| gaps(m.arrival(arrival(0, n, t + Duration::from_millis(1)))))
            .sum();
        assert_eq!(rejected, 1);
        m.close(t + Duration::from_millis(2));
        assert_eq!(
            gaps(m.arrival(arrival(0, 5_000, t + Duration::from_millis(3)))),
            1
        );
    }

    #[test]
    fn delivery_after_window_cannot_start_a_phantom_event() {
        // Live data: one provider delivered finalized accounts 13-35s late. The
        // late delivery became a new event (late provider on time with lag 0,
        // punctual provider missed) under the same event ID.
        let t = Instant::now();
        let mut m = Matcher::new(
            StreamKind {
                data_type: DataType::Account,
                commitment: Commitment::Finalized,
            },
            2,
            Duration::from_secs(5),
            CONNECTION_WARMUP,
            100,
            RETENTION,
            200,
        );
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        assert_eq!(
            lags(m.close(t + Duration::from_secs(5))),
            vec![(0, Outcome::OnTime, Some(0)), (1, Outcome::Missed, None)]
        );
        assert!(m.events.is_empty());
        for late in [6, 13, 35] {
            let at = t + Duration::from_secs(late);
            assert!(m.arrival(arrival(1, 1, at)).is_empty());
            assert!(m.close(at + Duration::from_secs(2)).is_empty());
        }
        // A repeated delivery from the punctual provider is also not a new event.
        assert!(m
            .arrival(arrival(0, 1, t + Duration::from_secs(40)))
            .is_empty());
        assert!(m.events.is_empty());
    }

    #[test]
    fn normal_expiry_reclaims_digests_but_never_reopens_an_old_key() {
        let t = Instant::now();
        let mut m = matcher(2, 4);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(0, 1, t));
        m.close(t + Duration::from_secs(4)); // closes at t + 1s
        let forgotten = t + Duration::from_secs(4) + RETENTION + Duration::from_secs(1);
        m.close(forgotten);
        assert!(m.expired.forget_at.is_empty());
        m.arrival(arrival(1, 1, forgotten));
        assert!(
            m.events.is_empty(),
            "a late replay cannot invent a new win or miss"
        );
        assert!(m.close(forgotten + Duration::from_secs(5)).is_empty());
        m.arrival(arrival(1, 2, forgotten));
        assert_eq!(m.events.len(), 1, "newer slots are still admitted");
    }

    #[test]
    fn expired_key_memory_is_bounded() {
        let t = Instant::now();
        let mut m = matcher(1, 2);
        connect_all(&mut m, 1, t);
        for n in 0..100 {
            let at = t + Duration::from_secs(n);
            m.arrival(arrival(0, n, at));
            m.close(at + Duration::from_secs(4));
        }
        assert!(m.expired.order.len() <= 4 && m.expired.forget_at.len() <= 4);
        // The newest expired keys are still remembered.
        assert!(m
            .arrival(arrival(0, 99, t + Duration::from_secs(110)))
            .is_empty());
        assert!(m.events.is_empty());
    }

    #[test]
    fn late_delivery_after_early_eviction_cannot_start_a_phantom_event() {
        // Review repro: 5 s window, 120 s retention, capacity 10, one new event
        // per second. A digest memory of 20 entries forgets the first key about
        // 20 s after it closes, long before its retention ends.
        let t = Instant::now();
        let mut m = Matcher::new(
            stream(),
            2,
            Duration::from_secs(5),
            CONNECTION_WARMUP,
            10,
            Duration::from_secs(120),
            20,
        );
        connect_all(&mut m, 2, t);
        for n in 1..=30 {
            let at = t + Duration::from_secs(n - 1);
            m.arrival(arrival(0, n, at));
            m.close(at);
        }
        assert!(m.events.len() < 10, "open windows never reached capacity");
        assert!(!m.expired.forget_at.contains_key(&digest(&key(1))));
        let late = t + Duration::from_secs(30);
        assert!(m.arrival(arrival(1, 1, late)).is_empty());
        assert!(!m.events.contains_key(&key(1)));
        // B never appears as the first (on-time) arrival of the old key.
        assert!(!lags(m.close(late + Duration::from_secs(6))).contains(&(
            1,
            Outcome::OnTime,
            Some(0)
        )));
    }

    #[test]
    fn slot_floor_rejects_only_keys_at_or_below_the_evicted_slot() {
        let t = Instant::now();
        let mut m = matcher(2, 1);
        connect_all(&mut m, 2, t);
        // Memory holds two digests; the third close forgets key 1 early.
        for n in 1..=3 {
            let at = t + Duration::from_secs(n);
            m.arrival(arrival(0, n, at));
            m.close(at + Duration::from_secs(1));
        }
        assert_eq!(m.expired.floors.get(&None), Some(&1));
        let at = t + Duration::from_secs(10);
        let other_key = |slot| Arrival {
            key: EventKey::Account {
                slot,
                pubkey: vec![2],
            },
            ..arrival(1, 0, at)
        };
        // An unseen key in the floor's slot is as old as the eviction horizon.
        assert!(m.arrival(other_key(1)).is_empty());
        assert!(m.events.is_empty());
        m.arrival(other_key(2));
        assert_eq!(m.events.len(), 1);
    }

    #[test]
    fn slot_status_floors_are_independent() {
        let t = Instant::now();
        let mut m = matcher(2, 1);
        connect_all(&mut m, 2, t);
        let status = |slot, status, at| Arrival {
            key: EventKey::Slot { slot, status },
            ..arrival(0, 0, at)
        };
        for n in 1..=3 {
            let at = t + Duration::from_secs(n);
            m.arrival(status(n, 0, at));
            m.close(at + Duration::from_secs(1));
        }
        assert_eq!(m.expired.floors.get(&Some(0)), Some(&1));
        // Finalized trails processed: a later status of an evicted slot is new.
        m.arrival(status(1, 2, t + Duration::from_secs(10)));
        assert_eq!(m.events.len(), 1);
    }

    #[test]
    fn disconnect_processed_after_a_later_arrival_cannot_create_a_miss() {
        // A stamps its disconnect at t, but B's arrival at t + 1ms is processed
        // first and opens the event while A still looks connected.
        let t = Instant::now();
        let mut m = matcher(2, 4);
        connect_all(&mut m, 2, t);
        m.arrival(arrival(1, 1, t + Duration::from_millis(1)));
        m.connection_change(0, t, SystemTime::UNIX_EPOCH, false, "dropped");
        assert_eq!(
            lags(m.close(t + Duration::from_secs(2))),
            vec![(1, Outcome::OnTime, Some(0))]
        );
    }

    #[test]
    fn gap_processed_after_a_later_arrival_invalidates_the_event() {
        // The dropped input at t may have been the true first arrival of an event
        // that B's later arrival opened before the gap was processed.
        for provider in [Some(0), None] {
            let t = Instant::now();
            let mut m = matcher(2, 4);
            connect_all(&mut m, 2, t);
            m.arrival(arrival(1, 1, t + Duration::from_millis(500)));
            m.gap(provider, t, SystemTime::UNIX_EPOCH, "arrival queue full");
            assert!(lags(m.close(t + Duration::from_secs(2))).is_empty());
        }
    }

    #[test]
    fn an_older_gap_never_hides_a_newer_one() {
        let t = Instant::now();
        let mut m = matcher(2, 4);
        connect_all(&mut m, 2, t);
        m.gap(
            None,
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
            "new",
        );
        m.gap(None, t, SystemTime::UNIX_EPOCH, "stale stamp");
        // Created after both gaps but stamped before the newer one: incomplete.
        m.arrival(arrival(0, 1, t + Duration::from_secs(1)));
        assert!(lags(m.close(t + Duration::from_secs(3))).is_empty());
    }
}
