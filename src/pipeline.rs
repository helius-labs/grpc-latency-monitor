//! Matching for one stream: arrivals and connection changes in, comparison rows
//! and certified coverage out. Readers only stamp and enqueue; everything that
//! decides a result happens here.
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

use crate::{
    config::Config,
    forks::{ForkFilter, SlotFates},
    matcher::Matcher,
    model::{
        Arrival, Commitment, CoverageRecord, DataType, EventKey, GapRecord, StorageRecord,
        StreamKind,
    },
    slots::{Reference, ReferenceCommitment, Rejection},
    stream::{InFlight, StreamControl},
};

/// How often healthy coverage is written per provider and stream. Pending
/// certified time is flushed as soon as health is lost, so batching never drops
/// certified time; it only bounds the health table to about one row per minute.
pub const COVERAGE_INTERVAL: Duration = Duration::from_secs(60);

/// Certified-healthy time for one provider that has not been written yet.
/// Consecutive rows overlap by one comparison window; do not sum their durations.
#[derive(Clone, Copy)]
struct Coverage {
    start: Instant,
    start_utc: SystemTime,
    /// UTC end of the latest clean tick, if not yet written.
    certified: Option<SystemTime>,
}

/// Matcher state for one stream, plus the health bookkeeping that depends on it.
struct StreamState {
    kind: StreamKind,
    matcher: Matcher,
    coverage: Vec<Option<Coverage>>,
    connected: Vec<bool>,
    connection_warmup: Duration,
    /// A stale arrival invalidates open comparisons at most once per tick:
    /// repeating the full scan per arrival would make a backlog quadratic.
    stale_invalidated: bool,
    storage_stalled: bool,
    /// Recovery from storage or reference gaps must not bridge unknown time.
    healthy_after: Option<(Instant, SystemTime)>,
    /// Slot streams record slot statuses; processed account and transaction
    /// streams hold their misses until the slot's fate is known.
    fates: Arc<SlotFates>,
    forks: Option<ForkFilter>,
    reference: watch::Receiver<Reference>,
    reference_ready: bool,
    reference_generation: u64,
    reference_config: crate::config::SlotValidationConfig,
    last_finalized_reference: Option<u64>,
    /// stale, future, reference unavailable; aggregated once per tick.
    rejections: Vec<[u64; 3]>,
    /// Per provider and reference commitment: out of bounds since the last
    /// in-bounds update using that tip. Unrelated slot statuses cannot clear it.
    out_of_bounds: Vec<[bool; ReferenceCommitment::COUNT]>,
    /// Observe transitions even when a dispute starts and clears between ticks.
    reference_was_disputed: bool,
}

impl StreamState {
    fn new(
        kind: StreamKind,
        count: usize,
        config: &Config,
        fates: Arc<SlotFates>,
        reference: watch::Receiver<Reference>,
    ) -> Self {
        let snapshot = *reference.borrow();
        let forks = (kind.commitment == Commitment::Processed && kind.data_type != DataType::Slot)
            .then(|| {
                ForkFilter::new(
                    config.slot_confirmation_timeout,
                    config.expired_key_capacity(),
                )
            });
        Self {
            kind,
            matcher: Matcher::new(
                kind,
                count,
                config.window,
                config.connection_warmup,
                config.matcher_key_capacity,
                config.expired_key_retention,
                config.expired_key_capacity(),
            ),
            coverage: vec![None; count],
            connected: vec![false; count],
            connection_warmup: config.connection_warmup,
            stale_invalidated: false,
            storage_stalled: false,
            healthy_after: snapshot.tips.map(|_| snapshot.healthy_since),
            fates,
            forks,
            reference,
            reference_ready: snapshot
                .fresh(Instant::now(), config.slot_validation.max_reference_age),
            reference_generation: snapshot.generation,
            reference_config: config.slot_validation.clone(),
            last_finalized_reference: None,
            rejections: vec![[0; 3]; count],
            out_of_bounds: vec![[false; ReferenceCommitment::COUNT]; count],
            reference_was_disputed: false,
        }
    }

    fn sync_reference(&mut self, now: Instant, utc: SystemTime) -> Vec<StorageRecord> {
        let snapshot = *self.reference.borrow_and_update();
        let ready = snapshot.fresh(now, self.reference_config.max_reference_age);
        let changed = snapshot.generation != self.reference_generation;
        let mut rows = Vec::new();
        if (self.reference_ready && !ready) || changed {
            rows.extend(self.lose_health(None));
            rows.extend(
                self.matcher
                    .invalidate_open(None, now, utc, "RPC slot reference gap"),
            );
        }
        if ready && (!self.reference_ready || changed) {
            // A coalesced outage/recovery must not bridge an old certificate.
            let recovered = if snapshot.healthy_since.0 > now {
                snapshot.healthy_since
            } else {
                (now, utc)
            };
            if self.healthy_after.is_none_or(|old| recovered.0 > old.0) {
                self.healthy_after = Some(recovered);
            }
        }
        if !ready {
            rows.extend(self.lose_health(None));
        } else if let Some(tips) = snapshot.tips {
            if self
                .last_finalized_reference
                .is_none_or(|old| tips.finalized > old)
            {
                self.fates.prune(tips.finalized);
                self.last_finalized_reference = Some(tips.finalized);
            }
        }
        self.reference_ready = ready;
        self.reference_generation = snapshot.generation;
        rows
    }

    /// A tip rejected by every connected provider withholds certification, even
    /// if other commitments on the same slot stream still admit updates.
    fn reference_disputed(&self) -> bool {
        self.connected.iter().any(|connected| *connected)
            && (0..ReferenceCommitment::COUNT).any(|commitment| {
                self.connected
                    .iter()
                    .zip(&self.out_of_bounds)
                    .filter(|(connected, _)| **connected)
                    .all(|(_, rejected)| rejected[commitment])
            })
    }

    fn sync_dispute(&mut self, now: Instant, utc: SystemTime) -> Vec<StorageRecord> {
        let disputed = self.reference_disputed();
        let mut rows = Vec::new();
        if disputed && !self.reference_was_disputed {
            rows.extend(self.lose_health(None));
            rows.extend(self.matcher.invalidate_open(
                None,
                now,
                utc,
                "RPC slot reference disputed",
            ));
        } else if !disputed && self.reference_was_disputed {
            // A tick certifies up to now - processing_grace. Its next interval
            // must start at recovery, not back inside the preceding dispute.
            if self.healthy_after.is_none_or(|old| now > old.0) {
                self.healthy_after = Some((now, utc));
            }
        }
        self.reference_was_disputed = disputed;
        rows
    }

    fn coverage_start(&self, at: Instant, utc: SystemTime) -> Coverage {
        let (start, start_utc) = self
            .healthy_after
            .filter(|(after, _)| *after > at)
            .unwrap_or((at, utc));
        Coverage {
            start,
            start_utc,
            certified: None,
        }
    }

    /// Observe saturation at the send itself, before capacity can recover. Keep
    /// accepted rows, flush only previously certified time, and withhold health
    /// until a subsequent clean tick after the last blocked send completes.
    async fn send_rows(
        &mut self,
        tx: &mpsc::Sender<StorageRecord>,
        rows: Vec<StorageRecord>,
    ) -> Result<()> {
        let mut rows = std::collections::VecDeque::from(rows);
        while let Some(row) = rows.pop_front() {
            match tx.try_send(row) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    anyhow::bail!("storage writer stopped")
                }
                Err(mpsc::error::TrySendError::Full(row)) => {
                    if !self.storage_stalled {
                        rows.extend(self.lose_health(None));
                        rows.push_back(StorageRecord::Gap(GapRecord {
                            stream: self.kind,
                            provider: None,
                            utc: SystemTime::now(),
                            reason: "storage queue full".into(),
                        }));
                    }
                    self.storage_stalled = true;
                    tx.send(row).await.context("storage writer stopped")?;
                    self.healthy_after = Some((Instant::now(), SystemTime::now()));
                }
            }
        }
        Ok(())
    }

    /// On shutdown, write certified time still pending and undecided misses (as
    /// forked). Open windows are not written: no coverage interval contains them.
    fn stop(&mut self) -> Vec<StorageRecord> {
        let mut rows = self.lose_health(None);
        if let Some(forks) = &mut self.forks {
            rows.extend(forks.drain());
        }
        rows
    }

    /// Close windows whose deadline has passed and release misses whose slot
    /// fate is now known.
    fn close(&mut self, end: Instant, now: Instant) -> Vec<StorageRecord> {
        // Draining or persisting queued arrivals may have blocked since the tick
        // captured `now`. Freshness must use the current clock at close itself.
        let mut out = self.sync_reference(Instant::now(), SystemTime::now());
        let rows = self.matcher.close(end);
        out.extend(match &mut self.forks {
            Some(forks) => forks.filter(rows, now, &self.fates),
            None => rows,
        });
        out
    }

    /// Stop certifying one provider (or all): first write any certified time that
    /// is still pending, since it was verified before health was lost.
    fn lose_health(&mut self, provider: Option<usize>) -> Vec<StorageRecord> {
        let providers = match provider {
            Some(index) => index..index + 1,
            None => 0..self.coverage.len(),
        };
        let mut rows = Vec::new();
        for provider in providers {
            if let Some(Coverage {
                start_utc,
                certified: Some(end),
                ..
            }) = self.coverage[provider].take()
            {
                rows.push(StorageRecord::Coverage(CoverageRecord {
                    stream: self.kind,
                    provider,
                    start: start_utc,
                    end,
                }));
            }
        }
        rows
    }

    fn control(&mut self, control: StreamControl) -> Vec<StorageRecord> {
        match control {
            StreamControl::Connected {
                provider,
                at,
                utc,
                reason,
                ..
            } => {
                let mut rows = self.lose_health(Some(provider));
                self.connected[provider] = true;
                self.out_of_bounds[provider] = [false; ReferenceCommitment::COUNT];
                rows.extend(self.sync_dispute(Instant::now(), SystemTime::now()));
                // Healthy coverage starts with miss eligibility, after warmup.
                self.coverage[provider] = (self.reference_ready && !self.reference_was_disputed)
                    .then(|| {
                        self.coverage_start(
                            at + self.connection_warmup,
                            utc + self.connection_warmup,
                        )
                    });
                rows.extend(
                    self.matcher
                        .connection_change(provider, at, utc, true, reason),
                );
                rows
            }
            StreamControl::Disconnected {
                provider,
                at,
                utc,
                reason,
                ..
            } => {
                self.connected[provider] = false;
                let mut rows = self.lose_health(Some(provider));
                rows.extend(self.sync_dispute(Instant::now(), SystemTime::now()));
                rows.extend(
                    self.matcher
                        .connection_change(provider, at, utc, false, reason),
                );
                rows
            }
            StreamControl::Gap {
                provider,
                at,
                utc,
                reason,
                ..
            } => {
                // The matcher invalidates every provider's open comparisons: the
                // dropped input may have been another provider's first arrival.
                let mut rows = self.lose_health(None);
                rows.extend(self.matcher.gap(Some(provider), at, utc, reason));
                rows
            }
        }
    }

    fn arrival(&mut self, arrival: Arrival, processing_grace: Duration) -> Vec<StorageRecord> {
        let now = Instant::now();
        let utc = SystemTime::now();
        let mut rows = self.sync_reference(now, utc);
        // Queueing delay is checked before slot bounds: a backlog ages slots below
        // the current tip, and must withhold health instead of blaming the provider.
        let stale = now.duration_since(arrival.at) > processing_grace;
        if stale {
            rows.extend(self.lose_health(None));
            if !self.stale_invalidated {
                self.stale_invalidated = true;
                rows.extend(self.matcher.invalidate_open(
                    None,
                    arrival.at,
                    arrival.utc,
                    "processing grace exceeded",
                ));
            }
        }
        let commitment = ReferenceCommitment::for_event(self.kind, &arrival.key).index();
        let check = self.reference.borrow().check(
            self.kind,
            &arrival.key,
            arrival.at,
            now,
            &self.reference_config,
        );
        if let Err(reason) = check {
            // Advancing the RPC tip must not discard an on-time peer of an
            // already-admitted key and invent a miss at the lower boundary.
            if reason != Rejection::Stale || !self.matcher.has_open(&arrival.key) {
                if !(reason == Rejection::Stale && stale) {
                    let index = match reason {
                        Rejection::Stale => 0,
                        Rejection::Future => 1,
                        Rejection::Unavailable => 2,
                    };
                    self.rejections[arrival.provider][index] =
                        self.rejections[arrival.provider][index].saturating_add(1);
                    if reason != Rejection::Unavailable
                        && !self.out_of_bounds[arrival.provider][commitment]
                    {
                        self.out_of_bounds[arrival.provider][commitment] = true;
                        rows.extend(self.sync_dispute(now, utc));
                    }
                }
                // A later peer may pass admission after the RPC tip advances or
                // recovers. It must not turn this discarded first delivery into
                // a win for that peer or a missed deadline for this provider.
                rows.extend(self.matcher.arrival_rejected(arrival));
                if rows.iter().any(|row| matches!(row, StorageRecord::Gap(_))) {
                    rows.extend(self.lose_health(None));
                }
                return rows;
            }
        }
        if check.is_ok() && self.out_of_bounds[arrival.provider][commitment] {
            self.out_of_bounds[arrival.provider][commitment] = false;
            rows.extend(self.sync_dispute(now, utc));
        }
        // A slot's fate is a fact regardless of whether this comparison is complete.
        if let EventKey::Slot { slot, status } = arrival.key {
            self.fates.record(slot, status);
        }
        if stale {
            rows.extend(self.matcher.arrival_incomplete(arrival));
        } else {
            rows.extend(self.matcher.arrival(arrival));
        }
        if rows.iter().any(|row| matches!(row, StorageRecord::Gap(_))) {
            rows.extend(self.lose_health(None));
        }
        rows
    }
}

// A matcher owns one stream. Control events have precedence over closing windows;
// arrivals must be drained before close, and the processing grace is ONLY applied
// to close scheduling, never to the matcher's configured comparison window.
// Healthy coverage is written at most once per `coverage_every` per provider.
#[allow(clippy::too_many_arguments)]
pub async fn match_stream(
    kind: StreamKind,
    count: usize,
    config: Config,
    mut arrivals: mpsc::Receiver<Arrival>,
    mut controls: mpsc::Receiver<StreamControl>,
    in_flight: Arc<InFlight>,
    fates: Arc<SlotFates>,
    reference: watch::Receiver<Reference>,
    mut stop: watch::Receiver<bool>,
    storage: mpsc::Sender<StorageRecord>,
    coverage_every: Duration,
) -> Result<()> {
    let mut state = StreamState::new(kind, count, &config, fates, reference);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let rows = state.sync_reference(Instant::now(), SystemTime::now());
        state.send_rows(&storage, rows).await?;
        // Drain lifecycle changes before considering arrivals or certification. A reader
        // sends its gap before reconnecting; a drop invalidates the whole pending interval.
        for _ in 0..64 {
            let Ok(control) = controls.try_recv() else {
                break;
            };
            let rows = state.control(control);
            state.send_rows(&storage, rows).await?;
        }
        tokio::select! {
            biased;
            Ok(()) = stop.changed() => {
                let rows = state.stop();
                state.send_rows(&storage, rows).await?;
                return Ok(());
            }
            changed = state.reference.changed() => {
                changed.context("reference RPC task stopped")?;
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let utc = SystemTime::now();
                let end = now - config.processing_grace;
                let end_utc = utc - config.processing_grace;
                // Check publications BEFORE draining the queues. An update or control
                // stamped before `end` is either still published here, or its reader
                // enqueued it before this check, so the drain below processes it.
                let stalled = in_flight.published_before(end);
                // Process everything already queued. A busy queue is normal under
                // load and must not discard open windows; only an unprocessable
                // stamp older than the cutoff can change a window closing now.
                for _ in 0..controls.len() {
                    let Ok(control) = controls.try_recv() else { break };
                    let rows = state.control(control);
                    state.send_rows(&storage, rows).await?;
                }
                for _ in 0..arrivals.len() {
                    let Ok(arrival) = arrivals.try_recv() else { break };
                    let rows = state.arrival(arrival, config.processing_grace);
                    state.send_rows(&storage, rows).await?;
                }
                if stalled {
                    let rows = state.matcher.invalidate_open(
                        None, now, utc, "reader stalled beyond processing grace",
                    );
                    state.send_rows(&storage, rows).await?;
                }
                let rows = state.close(end, now);
                state.send_rows(&storage, rows).await?;
                state.stale_invalidated = false;
                for (provider, counts) in state.rejections.iter_mut().enumerate() {
                    let [stale, future, unavailable] = std::mem::take(counts);
                    if stale > 0 || future > 0 || unavailable > 0 {
                        tracing::warn!(provider = %config.providers[provider].name,
                            data_type = kind.data_type.as_str(), commitment = kind.commitment.as_str(),
                            stale, future, reference_unavailable = unavailable, "slot validation rejected updates");
                    }
                }
                // A full storage queue delays rows but cannot change results; it
                // only withholds health certification.
                if !state.reference_ready
                    || state.reference_disputed()
                    || stalled
                    || state.storage_stalled
                    || storage.capacity() == 0
                {
                    state.storage_stalled = false;
                    let rows = state.lose_health(None);
                    state.send_rows(&storage, rows).await?;
                    continue;
                }
                for provider in 0..count {
                    let Some(mut coverage) = state.coverage[provider] else {
                        if state.connected[provider] {
                            // Begin afresh only after a fully drained tick; do not bridge
                            // the unknown interval to any earlier certificate. Never
                            // start before the provider's connection warmup ends.
                            let warmup = state.matcher.eligible_since(provider)
                                .map_or(Duration::ZERO, |since| since.saturating_duration_since(end));
                            state.coverage[provider] = Some(state.coverage_start(end + warmup, end_utc + warmup));
                        }
                        continue;
                    };
                    if coverage.start >= end {
                        continue;
                    }
                    if coverage.start_utc > end_utc {
                        let rows = state.lose_health(Some(provider));
                        state.send_rows(&storage, rows).await?;
                        let rows = state.matcher.invalidate_open(Some(provider), now, utc, "wall clock discontinuity");
                        state.send_rows(&storage, rows).await?;
                        continue;
                    }
                    coverage.certified = Some(end_utc);
                    let mut row = None;
                    if end.duration_since(coverage.start) >= coverage_every {
                        row = Some(StorageRecord::Coverage(CoverageRecord {
                            stream: kind, provider, start: coverage.start_utc, end: end_utc,
                        }));
                        coverage.certified = None;
                        if end.duration_since(coverage.start) >= config.window {
                            coverage.start = end - config.window;
                            coverage.start_utc = end_utc - config.window;
                        }
                    }
                    state.coverage[provider] = Some(coverage);
                    state.send_rows(&storage, row.into_iter().collect()).await?;
                    if state.storage_stalled { break; }
                }
            }
            control = controls.recv() => {
                let control = control.context("control channel closed")?;
                let rows = state.control(control);
                state.send_rows(&storage, rows).await?;
            }
            arrival = arrivals.recv() => {
                let arrival = arrival.context("arrival channel closed")?;
                let rows = state.arrival(arrival, config.processing_grace);
                state.send_rows(&storage, rows).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Outcome;
    use yellowstone_grpc_proto::geyser::SlotStatus;

    /// A live reference must advance to be renewed.
    fn advanced(old: &Reference) -> crate::slots::Tips {
        let mut tips = old.tips.unwrap();
        tips.processed += 1;
        tips
    }

    fn guarded_state(
        kind: StreamKind,
        count: usize,
    ) -> (Config, watch::Sender<Reference>, StreamState) {
        let mut config = Config::example();
        config.window = Duration::from_secs(1);
        config.connection_warmup = Duration::ZERO;
        let (tx, reference) = watch::channel(Reference::fixture(crate::slots::Tips {
            processed: 1040,
            confirmed: 1038,
            finalized: 1000,
        }));
        let state = StreamState::new(kind, count, &config, Arc::default(), reference);
        (config, tx, state)
    }

    #[test]
    fn future_events_never_open_valid_comparisons_or_poison_slot_fates() {
        for kind in [
            test_kind(),
            StreamKind {
                data_type: DataType::Transaction,
                commitment: Commitment::Finalized,
            },
        ] {
            let (config, _tx, mut state) = guarded_state(kind, 2);
            let at = Instant::now();
            let utc = SystemTime::now();
            for provider in 0..2 {
                state.control(connected(provider, at - Duration::from_secs(1), utc));
            }
            let key = if kind.data_type == DataType::Slot {
                EventKey::Slot {
                    slot: 1_000_000,
                    status: SlotStatus::SlotFinalized as i32,
                }
            } else {
                EventKey::Transaction {
                    slot: 1_000_000,
                    signature: vec![1],
                }
            };
            assert!(state
                .arrival(
                    Arrival {
                        provider: 0,
                        key,
                        at,
                        utc,
                        message_bytes: None
                    },
                    config.processing_grace
                )
                .is_empty());
            assert_eq!(state.fates.fate(1_000_000), crate::forks::Fate::Unknown);
            assert_eq!(state.rejections[0], [0, 1, 0]);
            assert!(state
                .close(at + Duration::from_secs(2), at + Duration::from_secs(2))
                .is_empty());
            // Rejection cannot advance a dedup floor and suppress a genuine event.
            let live = if kind.data_type == DataType::Slot {
                EventKey::Slot {
                    slot: 1000,
                    status: SlotStatus::SlotFinalized as i32,
                }
            } else {
                EventKey::Transaction {
                    slot: 1000,
                    signature: vec![2],
                }
            };
            state.arrival(
                Arrival {
                    provider: 0,
                    key: live,
                    at,
                    utc,
                    message_bytes: None,
                },
                config.processing_grace,
            );
            let rows = state.close(at + Duration::from_secs(2), at + Duration::from_secs(2));
            assert!(rows.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.provider == 1 && l.outcome == Outcome::Missed)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_first_delivery_cannot_reverse_results_across_an_rpc_boundary() {
        let kinds = StreamKind::ALL.into_iter().flat_map(|kind| {
            let statuses = if kind.data_type == DataType::Slot {
                vec![
                    Some(SlotStatus::SlotProcessed),
                    Some(SlotStatus::SlotConfirmed),
                    Some(SlotStatus::SlotFinalized),
                ]
            } else {
                vec![None]
            };
            statuses.into_iter().map(move |status| (kind, status))
        });
        for (kind, status) in kinds {
            for peer_delay in [Duration::from_millis(10), Duration::from_millis(1100)] {
                let (config, references, mut state) = guarded_state(kind, 2);
                let (at, utc) = (Instant::now(), SystemTime::now());
                for provider in 0..2 {
                    state.control(connected(provider, at - Duration::from_secs(1), utc));
                }
                let tips = references.borrow().tips.unwrap();
                let tip = match status {
                    Some(SlotStatus::SlotFinalized) => tips.finalized,
                    Some(SlotStatus::SlotConfirmed) => tips.confirmed,
                    Some(_) => tips.processed,
                    None if kind.commitment == Commitment::Finalized => tips.finalized,
                    None => tips.processed,
                };
                let key = |slot| match kind.data_type {
                    DataType::Slot => EventKey::Slot {
                        slot,
                        status: status.unwrap() as i32,
                    },
                    DataType::Account => EventKey::Account {
                        slot,
                        pubkey: vec![1; 32],
                        txn_signature: vec![1; 64],
                    },
                    DataType::Transaction => EventKey::Transaction {
                        slot,
                        signature: vec![1; 64],
                    },
                };
                let event = key(tip + config.slot_validation.slot_buffer + 1);
                let arrival = |provider, key| Arrival {
                    provider,
                    key,
                    at: Instant::now(),
                    utc: SystemTime::now(),
                    message_bytes: None,
                };
                state.arrival(arrival(0, event.clone()), config.processing_grace);
                assert_eq!(state.rejections[0], [0, 1, 0]);
                assert!(!state.reference_disputed());
                assert!(state.coverage.iter().all(Option::is_some));
                if peer_delay > config.window {
                    let end = at + config.window;
                    assert!(state.close(end, end).is_empty());
                }
                tokio::time::advance(peer_delay).await;
                let old = *references.borrow();
                let mut next = tips;
                next.processed += 1;
                next.confirmed += 1;
                next.finalized += 1;
                references.send_replace(
                    old.advance(
                        next,
                        Instant::now(),
                        Instant::now(),
                        SystemTime::now(),
                        config.slot_validation.max_reference_age,
                        config.slot_validation.slot_buffer,
                    )
                    .unwrap(),
                );
                state.arrival(arrival(1, event), config.processing_grace);
                let end = Instant::now() + config.window;
                assert!(
                    state.close(end, end).is_empty(),
                    "rejected A must not become missed while B wins: {kind:?}, {status:?}"
                );
                // Other valid keys still produce normal comparisons.
                for provider in 0..2 {
                    state.arrival(arrival(provider, key(tip + 2)), config.processing_grace);
                }
                let rows = state.close(end, end);
                assert_eq!(rows.iter().filter(|row| matches!(row, StorageRecord::Lag(l) if l.outcome == Outcome::OnTime)).count(),2);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_delivery_rejected_without_a_reference_stays_incomplete_after_recovery() {
        let (config, references, mut state) = guarded_state(test_kind(), 2);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        references.send_replace(Reference::empty());
        let arrival = |provider| Arrival {
            provider,
            key: EventKey::Slot {
                slot: 1040,
                status: SlotStatus::SlotProcessed as i32,
            },
            at: Instant::now(),
            utc: SystemTime::now(),
            message_bytes: None,
        };
        state.arrival(arrival(0), config.processing_grace);
        assert_eq!(state.rejections[0], [0, 0, 1]);
        tokio::time::advance(Duration::from_millis(10)).await;
        let old = *references.borrow();
        references.send_replace(
            old.advance(
                crate::slots::Tips {
                    processed: 1040,
                    confirmed: 1038,
                    finalized: 1000,
                },
                Instant::now(),
                Instant::now(),
                SystemTime::now(),
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap(),
        );
        state.arrival(arrival(1), config.processing_grace);
        let end = Instant::now() + config.window;
        assert!(state.close(end, end).is_empty());
    }

    #[test]
    fn a_rejected_only_marker_cannot_bypass_the_stale_bound() {
        let (config, references, mut state) = guarded_state(test_kind(), 2);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        let arrival = |provider| Arrival {
            provider,
            key: EventKey::Slot {
                slot: 1026,
                status: SlotStatus::SlotFinalized as i32,
            },
            at,
            utc,
            message_bytes: None,
        };
        state.arrival(arrival(0), config.processing_grace);
        assert_eq!(state.rejections[0], [0, 1, 0]);
        let old = *references.borrow();
        references.send_replace(
            old.advance(
                crate::slots::Tips {
                    processed: 1100,
                    confirmed: 1080,
                    finalized: 1052,
                },
                at,
                at,
                utc,
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap(),
        );
        state.arrival(arrival(1), config.processing_grace);
        assert_eq!(state.rejections[1], [1, 0, 0]);
        assert_eq!(state.fates.fate(1026), crate::forks::Fate::Unknown);
    }

    #[test]
    fn a_reference_rejecting_every_provider_withholds_certification() {
        let (config, _tx, mut state) = guarded_state(test_kind(), 2);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        let arrival = |provider, slot| Arrival {
            provider,
            key: EventKey::Slot { slot, status: 0 },
            at,
            utc,
            message_bytes: None,
        };
        // One provider out of bounds is that provider's problem, not the reference's.
        state.arrival(arrival(0, 2000), config.processing_grace);
        assert!(!state.reference_disputed());
        // Every connected provider out of bounds means the reference lags the chain.
        state.arrival(arrival(1, 2001), config.processing_grace);
        assert!(state.reference_disputed());
        assert_eq!(state.rejections, [[0, 1, 0], [0, 1, 0]]);
        // An in-bounds update using the same tip clears its provider's flag.
        state.arrival(arrival(1, 1040), config.processing_grace);
        assert!(!state.reference_disputed());
    }

    #[test]
    fn accepted_slot_statuses_cannot_mask_a_dispute_about_another_tip() {
        for (status, live_slot, other_status, other_slot) in [
            (
                SlotStatus::SlotConfirmed,
                1038,
                SlotStatus::SlotFinalized,
                1000,
            ),
            (
                SlotStatus::SlotFinalized,
                1000,
                SlotStatus::SlotConfirmed,
                1038,
            ),
        ] {
            let (config, _tx, mut state) = guarded_state(test_kind(), 2);
            let (at, utc) = (Instant::now(), SystemTime::now());
            for provider in 0..2 {
                state.control(connected(provider, at - Duration::from_secs(1), utc));
            }
            let arrival = |provider, slot, status| Arrival {
                provider,
                key: EventKey::Slot {
                    slot,
                    status: status as i32,
                },
                at,
                utc,
                message_bytes: None,
            };
            state.arrival(arrival(0, 1100, status), config.processing_grace);
            assert!(!state.reference_disputed());
            assert!(state.coverage.iter().all(Option::is_some));
            state.arrival(arrival(1, 1100, status), config.processing_grace);
            assert!(state.reference_disputed());
            assert!(state.coverage.iter().all(Option::is_none));
            for provider in 0..2 {
                state.arrival(
                    arrival(provider, 1040, SlotStatus::SlotProcessed),
                    config.processing_grace,
                );
                state.arrival(
                    arrival(provider, other_slot, other_status),
                    config.processing_grace,
                );
                assert!(state.reference_disputed());
            }
            state.arrival(arrival(0, live_slot, status), config.processing_grace);
            assert!(!state.reference_disputed());
        }
    }

    #[test]
    fn a_dispute_requires_the_same_tip_and_only_connected_providers_vote() {
        let (config, _tx, mut state) = guarded_state(test_kind(), 3);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        let arrival = |provider, status| Arrival {
            provider,
            key: EventKey::Slot {
                slot: 1100,
                status: status as i32,
            },
            at,
            utc,
            message_bytes: None,
        };
        state.arrival(
            arrival(0, SlotStatus::SlotFinalized),
            config.processing_grace,
        );
        state.arrival(
            arrival(1, SlotStatus::SlotConfirmed),
            config.processing_grace,
        );
        assert!(!state.reference_disputed());
        // Removing the only provider without a finalized rejection exposes the
        // dispute; disconnected provider 2 must not hide it.
        state.control(StreamControl::Disconnected {
            provider: 1,
            stream: test_kind(),
            at,
            utc,
            reason: "fixture disconnect".into(),
        });
        assert!(state.reference_disputed());
        assert!(state.coverage.iter().all(Option::is_none));
        // Reconnection resets every commitment flag for that provider.
        state.control(connected(0, at, utc));
        assert!(!state.reference_disputed());
        assert_eq!(state.out_of_bounds[0], [false; ReferenceCommitment::COUNT]);
        let coverage = state.coverage[0].unwrap();
        assert!(coverage.start >= state.healthy_after.unwrap().0);
    }

    #[tokio::test(start_paused = true)]
    async fn dispute_recovery_between_ticks_cannot_backdate_coverage() {
        let (config, _tx, mut state) = guarded_state(test_kind(), 2);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
            state.coverage[provider].as_mut().unwrap().certified = Some(utc);
        }
        let arrival = |provider, slot| Arrival {
            provider,
            key: EventKey::Slot {
                slot,
                status: SlotStatus::SlotFinalized as i32,
            },
            at: Instant::now(),
            utc: SystemTime::now(),
            message_bytes: None,
        };
        state.arrival(arrival(0, 1100), config.processing_grace);
        let rows = state.arrival(arrival(1, 1100), config.processing_grace);
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row, StorageRecord::Coverage(c) if c.end == utc))
                .count(),
            2
        );
        assert!(rows.iter().any(|row| matches!(row,
            StorageRecord::Gap(g) if g.reason == "RPC slot reference disputed")));
        tokio::time::advance(Duration::from_millis(500)).await;
        let recovery = Instant::now();
        state.arrival(arrival(0, 1000), config.processing_grace);
        assert!(!state.reference_disputed());
        // No certification tick saw the dispute, but its recovery boundary must
        // still clamp a new interval whose usual start is now - processing_grace.
        let coverage = state.coverage_start(
            Instant::now() - config.processing_grace,
            SystemTime::now() - config.processing_grace,
        );
        assert_eq!(coverage.start, recovery);
        assert_eq!(coverage.start_utc, state.healthy_after.unwrap().1);
        // A later recovery supersedes the previous boundary.
        state.arrival(arrival(0, 1100), config.processing_grace);
        tokio::time::advance(Duration::from_millis(500)).await;
        state.arrival(arrival(1, 1000), config.processing_grace);
        assert!(state.healthy_after.unwrap().0 > recovery);
    }

    #[tokio::test]
    async fn mixed_slot_traffic_withholds_coverage_until_dispute_recovery() {
        let (mut config, references, _) = guarded_state(test_kind(), 2);
        config.processing_grace = Duration::from_millis(500);
        config.slot_validation.max_reference_age = Duration::from_secs(60);
        let (arrivals_tx, arrivals) = mpsc::channel(16);
        let (controls_tx, controls) = mpsc::channel(8);
        let (storage, mut rows) = mpsc::channel(128);
        let (stop, shutdown) = watch::channel(false);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            controls_tx
                .try_send(connected(
                    provider,
                    at - Duration::from_secs(1),
                    utc - Duration::from_secs(1),
                ))
                .unwrap();
        }
        let task = tokio::spawn(super::match_stream(
            test_kind(),
            2,
            config,
            arrivals,
            controls,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            references.subscribe(),
            shutdown,
            storage,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        // Discard startup certificates and connection records.
        while rows.try_recv().is_ok() {}
        tokio::time::sleep(Duration::from_millis(500)).await;
        let arrival = |provider, slot, status| Arrival {
            provider,
            key: EventKey::Slot {
                slot,
                status: status as i32,
            },
            at: Instant::now(),
            utc: SystemTime::now(),
            message_bytes: None,
        };
        for provider in 0..2 {
            arrivals_tx
                .try_send(arrival(provider, 1100, SlotStatus::SlotFinalized))
                .unwrap();
        }
        for provider in 0..2 {
            arrivals_tx
                .try_send(arrival(provider, 1040, SlotStatus::SlotProcessed))
                .unwrap();
        }
        tokio::task::yield_now().await;
        let disputed: Vec<_> = std::iter::from_fn(|| rows.try_recv().ok()).collect();
        assert!(disputed.iter().any(|row| matches!(row,
            StorageRecord::Gap(g) if g.reason == "RPC slot reference disputed")));
        // A clean tick with valid processed traffic still cannot certify the
        // stream while both providers' finalized updates are out of bounds.
        tokio::time::sleep(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let disputed: Vec<_> = std::iter::from_fn(|| rows.try_recv().ok()).collect();
        assert!(!disputed.iter().any(|row| matches!(
            row,
            StorageRecord::Coverage(_) | StorageRecord::Connection(_)
        )));
        let recovery = arrival(0, 1000, SlotStatus::SlotFinalized);
        let recovered_utc = recovery.utc;
        arrivals_tx.try_send(recovery).unwrap();
        tokio::task::yield_now().await;
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        let recovered: Vec<_> = std::iter::from_fn(|| rows.try_recv().ok()).collect();
        let coverage: Vec<_> = recovered
            .iter()
            .filter_map(|row| match row {
                StorageRecord::Coverage(c) => Some(c),
                _ => None,
            })
            .collect();
        assert!(!coverage.is_empty());
        assert!(coverage.iter().all(|c| c.start >= recovered_utc));
        assert!(!recovered
            .iter()
            .any(|row| matches!(row, StorageRecord::Connection(_))));
    }

    #[test]
    fn a_matcher_backlog_loses_health_instead_of_blaming_the_provider() {
        let (config, _tx, mut state) = guarded_state(test_kind(), 2);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(60), utc));
        }
        assert!(state.coverage.iter().all(Option::is_some));
        // Delivered on time 20 s ago; its slot is now below the current tip - 25.
        let delayed = at - Duration::from_secs(20);
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1000,
                    status: 0,
                },
                at: delayed,
                utc: utc - Duration::from_secs(20),
                message_bytes: None,
            },
            config.processing_grace,
        );
        assert!(state.coverage.iter().all(Option::is_none));
        assert_eq!(state.rejections[0], [0, 0, 0]);
        assert_eq!(state.out_of_bounds[0], [false; ReferenceCommitment::COUNT]);
    }

    #[test]
    fn future_finalization_cannot_turn_a_confirmed_miss_into_a_fork() {
        let (config, tx, mut slots) = guarded_state(test_kind(), 2);
        let kind = StreamKind {
            data_type: DataType::Transaction,
            commitment: Commitment::Processed,
        };
        let mut transactions =
            StreamState::new(kind, 2, &config, slots.fates.clone(), tx.subscribe());
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            transactions.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        let arrival = |key| Arrival {
            provider: 0,
            key,
            at,
            utc,
            message_bytes: None,
        };
        slots.arrival(
            arrival(EventKey::Slot {
                slot: 1038,
                status: SlotStatus::SlotConfirmed as i32,
            }),
            config.processing_grace,
        );
        slots.arrival(
            arrival(EventKey::Slot {
                slot: 1_000_000,
                status: SlotStatus::SlotFinalized as i32,
            }),
            config.processing_grace,
        );
        assert_eq!(slots.fates.fate(1038), crate::forks::Fate::Confirmed);
        transactions.arrival(
            arrival(EventKey::Transaction {
                slot: 1038,
                signature: vec![1],
            }),
            config.processing_grace,
        );
        let rows = transactions.close(at + Duration::from_secs(2), at + Duration::from_secs(2));
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.provider == 1 && l.outcome == Outcome::Missed)));
        assert!(!rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Lag(l) if l.outcome == Outcome::Forked)));
    }

    #[test]
    fn an_on_time_peer_is_not_lost_when_the_rpc_lower_boundary_advances() {
        let (config, references, mut state) = guarded_state(test_kind(), 2);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        let key = EventKey::Slot {
            slot: 1015,
            status: 0,
        }; // exactly tip - 25
        state.arrival(
            Arrival {
                provider: 0,
                key: key.clone(),
                at,
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        let old = *references.borrow();
        let mut tips = old.tips.unwrap();
        tips.processed += 1;
        references.send_replace(
            old.advance(
                tips,
                at,
                at,
                utc,
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap(),
        );
        state.arrival(
            Arrival {
                provider: 1,
                key,
                at: at + Duration::from_millis(10),
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        let rows = state.close(at + Duration::from_secs(2), at + Duration::from_secs(2));
        let outcomes: Vec<_> = rows
            .iter()
            .filter_map(|r| match r {
                StorageRecord::Lag(l) => Some(l.outcome),
                _ => None,
            })
            .collect();
        assert_eq!(outcomes, [Outcome::OnTime, Outcome::OnTime]);
        // The same old slot cannot open a different comparison.
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1015,
                    status: SlotStatus::SlotCreatedBank as i32,
                },
                at,
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        assert_eq!(state.rejections[0], [1, 0, 0]);
        assert!(state
            .close(at + Duration::from_secs(3), at + Duration::from_secs(3))
            .is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn rpc_expiry_invalidates_open_windows_and_recovery_restarts_coverage() {
        let (config, tx, mut state) = guarded_state(test_kind(), 2);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            state.control(connected(provider, at - Duration::from_secs(1), utc));
        }
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1040,
                    status: 0,
                },
                at,
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        tokio::time::advance(config.slot_validation.max_reference_age).await;
        // Closing uses actual reference age even when the tick's drain was slow.
        let tick = at + Duration::from_secs(2);
        let rows = state.close(tick, tick);
        assert!(rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Gap(g) if g.reason == "RPC slot reference gap")));
        assert!(!rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Lag(_) | StorageRecord::Connection(_))));
        assert!(state.coverage.iter().all(Option::is_none));
        assert!(!state.reference_ready);
        let old = *tx.borrow();
        let recovered = old
            .advance(
                advanced(&old),
                Instant::now(),
                Instant::now(),
                SystemTime::now(),
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap();
        tx.send_replace(recovered);
        state.sync_reference(Instant::now(), SystemTime::now());
        assert!(state.reference_ready);
        let coverage = state.coverage_start(at, utc);
        assert!(coverage.start >= recovered.healthy_since.0);
        // Work stamped during the gap cannot be admitted after recovery.
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1041,
                    status: 0,
                },
                at,
                utc,
                message_bytes: None,
            },
            Duration::MAX,
        );
        assert_eq!(state.rejections[0], [0, 0, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_coalesced_rpc_outage_cannot_bridge_certified_coverage() {
        let (config, tx, mut state) = guarded_state(test_kind(), 2);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            state.control(connected(provider, at, utc));
        }
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1040,
                    status: 0,
                },
                at,
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        // The poller recovers before this busy matcher observes expiry.
        tokio::time::advance(config.slot_validation.max_reference_age + Duration::from_secs(1))
            .await;
        let old = *tx.borrow();
        tx.send_replace(
            old.advance(
                advanced(&old),
                Instant::now(),
                Instant::now(),
                SystemTime::now(),
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap(),
        );
        let rows = state.close(Instant::now(), Instant::now());
        assert!(state.reference_ready);
        assert!(state.coverage.iter().all(Option::is_none));
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Gap(_))));
        assert!(!rows.iter().any(|r| matches!(r, StorageRecord::Lag(_))));
    }

    #[test]
    fn startup_without_an_rpc_reference_cannot_score_or_certify() {
        let config = Config::example();
        let (_tx, reference) = watch::channel(Reference::empty());
        let mut state = StreamState::new(test_kind(), 2, &config, Arc::default(), reference);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            state.control(connected(provider, at, utc));
        }
        state.arrival(
            Arrival {
                provider: 0,
                key: EventKey::Slot {
                    slot: 1000,
                    status: 0,
                },
                at,
                utc,
                message_bytes: None,
            },
            config.processing_grace,
        );
        assert!(!state.reference_ready);
        assert!(state.coverage.iter().all(Option::is_none));
        assert!(state
            .close(at + config.window, at + config.window)
            .is_empty());
        assert_eq!(state.fates.fate(1000), crate::forks::Fate::Unknown);
    }

    #[tokio::test(start_paused = true)]
    async fn reference_expiry_and_recovery_work_through_the_running_pipeline() {
        let (mut config, references, _) = guarded_state(test_kind(), 2);
        config.processing_grace = Duration::from_millis(10);
        let (arrivals_tx, arrivals) = mpsc::channel(8);
        let (controls_tx, controls) = mpsc::channel(8);
        let (storage, mut rows) = mpsc::channel(128);
        let (stop, shutdown) = watch::channel(false);
        let at = Instant::now();
        let utc = SystemTime::now();
        for provider in 0..2 {
            controls_tx
                .try_send(connected(
                    provider,
                    at - Duration::from_secs(1),
                    utc - Duration::from_secs(1),
                ))
                .unwrap();
        }
        let task = tokio::spawn(super::match_stream(
            test_kind(),
            2,
            config.clone(),
            arrivals,
            controls,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            references.subscribe(),
            shutdown,
            storage,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        while rows.try_recv().is_ok() {}
        tokio::time::advance(Duration::from_millis(4500)).await;
        tokio::task::yield_now().await;
        while rows.try_recv().is_ok() {}
        let arrival = |slot| Arrival {
            provider: 0,
            key: EventKey::Slot { slot, status: 0 },
            at: Instant::now(),
            utc: SystemTime::now(),
            message_bytes: None,
        };
        arrivals_tx.try_send(arrival(1040)).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1500)).await;
        tokio::task::yield_now().await;
        let expired: Vec<_> = std::iter::from_fn(|| rows.try_recv().ok()).collect();
        assert!(expired
            .iter()
            .any(|r| matches!(r, StorageRecord::Gap(g) if g.reason == "RPC slot reference gap")));
        assert!(!expired.iter().any(|r| matches!(
            r,
            StorageRecord::Lag(_) | StorageRecord::Coverage(_) | StorageRecord::Connection(_)
        )));
        let old = *references.borrow();
        references.send_replace(
            old.advance(
                advanced(&old),
                Instant::now(),
                Instant::now(),
                SystemTime::now(),
                config.slot_validation.max_reference_age,
                config.slot_validation.slot_buffer,
            )
            .unwrap(),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        arrivals_tx.try_send(arrival(1041)).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        let recovered: Vec<_> = std::iter::from_fn(|| rows.try_recv().ok()).collect();
        assert!(recovered.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.key.slot() == 1041 && l.provider == 1 && l.outcome == Outcome::Missed)));
        assert!(!recovered
            .iter()
            .any(|r| matches!(r, StorageRecord::Connection(_))));
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    // Existing pipeline fixtures exercise scheduling, not admission bounds.
    fn fixture_reference(
        config: &mut Config,
    ) -> (watch::Sender<Reference>, watch::Receiver<Reference>) {
        config.slot_validation.slot_buffer = u64::MAX;
        config.slot_validation.max_reference_age = Duration::from_secs(86400);
        watch::channel(Reference::fixture(crate::slots::Tips {
            processed: 1000,
            confirmed: 1000,
            finalized: 1000,
        }))
    }

    fn test_state(
        kind: StreamKind,
        count: usize,
        config: &Config,
        fates: Arc<SlotFates>,
    ) -> StreamState {
        let mut config = config.clone();
        let (_tx, reference) = fixture_reference(&mut config);
        StreamState::new(kind, count, &config, fates, reference)
    }

    #[allow(clippy::too_many_arguments)]
    async fn match_stream(
        kind: StreamKind,
        count: usize,
        mut config: Config,
        arrivals: mpsc::Receiver<Arrival>,
        controls: mpsc::Receiver<StreamControl>,
        in_flight: Arc<InFlight>,
        fates: Arc<SlotFates>,
        stop: watch::Receiver<bool>,
        storage: mpsc::Sender<StorageRecord>,
        coverage_every: Duration,
    ) -> Result<()> {
        let (_tx, reference) = fixture_reference(&mut config);
        super::match_stream(
            kind,
            count,
            config,
            arrivals,
            controls,
            in_flight,
            fates,
            reference,
            stop,
            storage,
            coverage_every,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn storage_stall_breaks_coverage_even_when_capacity_recovers_before_tick() {
        let config = Config::example();
        let mut state = test_state(test_kind(), 2, &config, Arc::default());
        let at = Instant::now() - Duration::from_secs(20);
        let utc = SystemTime::now() - Duration::from_secs(20);
        for provider in 0..2 {
            state.control(connected(provider, at, utc));
            state.coverage[provider].as_mut().unwrap().certified =
                Some(utc + Duration::from_secs(10));
        }
        let (tx, mut rx) = mpsc::channel(8);
        let row = StorageRecord::Gap(GapRecord {
            stream: test_kind(),
            provider: None,
            utc,
            reason: "fixture".into(),
        });
        for _ in 0..8 {
            tx.try_send(row.clone()).unwrap();
        }
        {
            let pending = state.send_rows(&tx, vec![row]);
            tokio::pin!(pending);
            assert!(tokio::time::timeout(Duration::from_secs(30), &mut pending)
                .await
                .is_err());
            while rx.try_recv().is_ok() {}
            pending.await.unwrap();
        }
        assert!(tx.capacity() > 0, "a later capacity check misses the stall");
        assert!(state.storage_stalled);
        assert!(state.coverage.iter().all(Option::is_none));
        let recovered = state.coverage_start(at, utc);
        assert!(recovered.start >= Instant::now());
        let rows: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(
            rows.iter()
                .filter(|r| matches!(r, StorageRecord::Coverage(c)
            if c.end == utc + Duration::from_secs(10)))
                .count(),
            2
        );
        assert!(rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Gap(g) if g.reason == "storage queue full")));
    }

    fn test_kind() -> StreamKind {
        StreamKind {
            data_type: DataType::Slot,
            commitment: Commitment::Processed,
        }
    }

    fn connected(provider: usize, at: Instant, utc: SystemTime) -> StreamControl {
        StreamControl::Connected {
            provider,
            stream: test_kind(),
            at,
            utc,
            reason: "subscribed".into(),
        }
    }

    #[tokio::test]
    async fn full_queue_control_precedes_close_and_suppresses_uncertified_miss() {
        let mut config = Config::example();
        config.window = Duration::from_millis(10);
        config.processing_grace = Duration::from_millis(10);
        let kind = StreamKind {
            data_type: DataType::Slot,
            commitment: Commitment::Processed,
        };
        let (arrivals_tx, arrivals_rx) = mpsc::channel(1);
        let (controls_tx, controls_rx) = mpsc::channel(config.providers.len() * 4);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let at = Instant::now() - Duration::from_secs(2);
        let utc = SystemTime::now() - Duration::from_secs(2);
        for provider in 0..2 {
            controls_tx
                .try_send(StreamControl::Connected {
                    provider,
                    stream: kind,
                    at,
                    utc,
                    reason: "subscribed".into(),
                })
                .unwrap();
        }
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot { slot: 1, status: 0 },
                at,
                utc,
            })
            .unwrap();
        assert!(arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 1,
                key: EventKey::Slot { slot: 2, status: 0 },
                at,
                utc
            })
            .is_err());
        controls_tx
            .try_send(StreamControl::Gap {
                provider: 1,
                stream: kind,
                at,
                utc,
                reason: "arrival queue full".into(),
            })
            .unwrap();
        controls_tx
            .try_send(StreamControl::Disconnected {
                provider: 1,
                stream: kind,
                at,
                utc,
                reason: "arrival queue full".into(),
            })
            .unwrap();
        let task = tokio::spawn(match_stream(
            kind,
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut rows = Vec::new();
        while let Ok(row) = storage_rx.try_recv() {
            rows.push(row);
        }
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Gap(_))));
        assert!(!rows.iter().any(|r| matches!(r, StorageRecord::Lag(_))));
        assert!(!rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Coverage(c) if c.provider == 1)));
        task.abort();
    }
    #[tokio::test(start_paused = true)]
    async fn stamped_in_flight_update_blocks_clean_close_until_queue_send() {
        let mut config = Config::example();
        config.window = Duration::from_millis(100);
        config.processing_grace = Duration::from_millis(10);
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let in_flight = Arc::new(InFlight::new(2));
        let connected_at = Instant::now() - Duration::from_secs(1);
        let connected_utc = SystemTime::now() - Duration::from_secs(1);
        for provider in 0..2 {
            controls_tx
                .try_send(connected(provider, connected_at, connected_utc))
                .unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            in_flight.clone(),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await; // initial idle tick
        while storage_rx.try_recv().is_ok() {} // discard initial connected/coverage rows
        let first = Instant::now();
        let first_utc = SystemTime::now();
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot {
                    slot: 17,
                    status: 0,
                },
                at: first,
                utc: first_utc,
            })
            .unwrap();
        tokio::task::yield_now().await;
        // Reader B stamps its in-window observation, then is descheduled for
        // longer than the processing grace before queueing it. Both channels
        // remain empty; only the published stamp reveals the pending input.
        tokio::time::advance(Duration::from_millis(50)).await;
        let stamp = in_flight.stamp(1);
        let second = stamp.at();
        let second_utc = SystemTime::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let tick_rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(tick_rows.iter().any(
            |r| matches!(r, StorageRecord::Gap(g) if g.reason == "reader stalled beyond processing grace")
        ));
        assert!(!tick_rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Lag(_) | StorageRecord::Coverage(_))));
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 1,
                key: EventKey::Slot {
                    slot: 17,
                    status: 0,
                },
                at: second,
                utc: second_utc,
            })
            .unwrap();
        drop(stamp); // reader clears after try_send
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(!rows.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.key == EventKey::Slot { slot: 17, status: 0 })));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_readers_still_finalize_and_certify_healthy_events() {
        let mut config = Config::example();
        config.window = Duration::from_millis(100);
        config.processing_grace = Duration::from_millis(10);
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let warmup = Config::example().connection_warmup;
        let at = Instant::now() - warmup - Duration::from_secs(1);
        let utc = SystemTime::now() - warmup - Duration::from_secs(1);
        for provider in 0..2 {
            controls_tx.try_send(connected(provider, at, utc)).unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        let at = Instant::now();
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot {
                    slot: 18,
                    status: 0,
                },
                at,
                utc: SystemTime::now(),
            })
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.key == EventKey::Slot { slot: 18, status: 0 } && l.provider == 0 && l.outcome == Outcome::OnTime)));
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Lag(l) if l.key == EventKey::Slot { slot: 18, status: 0 } && l.provider == 1 && l.outcome == Outcome::Missed)));
        assert!(rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Coverage(c) if c.provider == 1)));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_new_event_cannot_miss_peer_or_certify_wrong_lag() {
        let mut config = Config::example();
        config.window = Duration::from_secs(1);
        config.processing_grace = Duration::from_millis(10);
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let at = Instant::now() - Duration::from_secs(3);
        let utc = SystemTime::now() - Duration::from_secs(3);
        for provider in 0..2 {
            controls_tx.try_send(connected(provider, at, utc)).unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await; // initial clean tick, no arrival backlog
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot { slot: 9, status: 0 },
                at,
                utc,
            })
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await; // closes incomplete key
        tokio::task::yield_now().await;
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 1,
                key: EventKey::Slot { slot: 9, status: 0 },
                at: at + Duration::from_millis(500),
                utc: utc + Duration::from_millis(500),
            })
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(rows.iter().any(|r| matches!(r, StorageRecord::Gap(g) if g.reason == "processing grace exceeded" && g.provider.is_none())));
        assert!(!rows.iter().any(
            |r| matches!(r, StorageRecord::Lag(l) if l.key == EventKey::Slot { slot: 9, status: 0 })
        ));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn backlog_recovers_coverage_without_reconnecting() {
        let mut config = Config::example();
        config.processing_grace = Duration::from_millis(10);
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let at = Instant::now() - Duration::from_secs(2);
        let utc = SystemTime::now() - Duration::from_secs(2);
        controls_tx.try_send(connected(0, at, utc)).unwrap();
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot { slot: 1, status: 0 },
                at,
                utc,
            })
            .unwrap();
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        assert!(storage_rx.try_recv().is_ok());
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(rows.iter().any(
            |r| matches!(r, StorageRecord::Gap(g) if g.reason == "processing grace exceeded")
        ));
        assert!(rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Coverage(c) if c.provider == 0 && c.start > utc)));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn busy_queue_at_tick_is_drained_instead_of_invalidating_windows() {
        // Live data: ticks that found any queued arrival discarded every open
        // window (~3s of events each, ~12% of processed data, biased to bursts).
        let mut config = Config::example();
        config.window = Duration::from_millis(100);
        config.processing_grace = Duration::from_secs(2);
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(64);
        let at = Instant::now() - Duration::from_secs(1);
        let utc = SystemTime::now() - Duration::from_secs(1);
        for provider in 0..2 {
            controls_tx.try_send(connected(provider, at, utc)).unwrap();
        }
        let first = Instant::now();
        let first_utc = SystemTime::now();
        let key = EventKey::Slot {
            slot: 21,
            status: 0,
        };
        // Both observations are already queued when the interval's immediate
        // first tick fires, so the tick (biased first) finds a busy queue.
        for (provider, delay_ms) in [(0, 0), (1, 3)] {
            arrivals_tx
                .try_send(Arrival {
                    message_bytes: None,
                    provider,
                    key: key.clone(),
                    at: first + Duration::from_millis(delay_ms),
                    utc: first_utc,
                })
                .unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await; // close after the grace
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        // Paused test time advances Instant but not SystemTime, so coverage reports a
        // per-provider wall-clock discontinuity here; no window may be invalidated.
        assert!(!rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Gap(g) if g.provider.is_none())));
        let lags: Vec<_> = rows
            .iter()
            .filter_map(|r| match r {
                StorageRecord::Lag(l) if l.key == key => Some((l.provider, l.outcome, l.lag_ns)),
                _ => None,
            })
            .collect();
        assert_eq!(
            lags,
            vec![
                (0, Outcome::OnTime, Some(0)),
                (1, Outcome::OnTime, Some(3_000_000))
            ]
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_close_reclaims_expired_keys_while_arrivals_are_ready() {
        let mut config = Config::example();
        config.window = Duration::from_millis(10);
        config.processing_grace = Duration::from_millis(1);
        config.matcher_key_capacity = 1;
        let (arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        let at = Instant::now() - Duration::from_secs(2);
        let utc = SystemTime::now() - Duration::from_secs(2);
        controls_tx.try_send(connected(0, at, utc)).unwrap();
        arrivals_tx
            .try_send(Arrival {
                message_bytes: None,
                provider: 0,
                key: EventKey::Slot { slot: 1, status: 0 },
                at,
                utc,
            })
            .unwrap();
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let at = Instant::now();
        let utc = SystemTime::now();
        for _ in 0..8 {
            arrivals_tx
                .try_send(Arrival {
                    message_bytes: None,
                    provider: 0,
                    key: EventKey::Slot { slot: 2, status: 0 },
                    at,
                    utc,
                })
                .unwrap();
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert_eq!(
            arrivals_tx.capacity(),
            8,
            "all queued arrivals were drained"
        );
        assert!(!rows.iter().any(|r| matches!(r, StorageRecord::Lag(_))));
        assert!(!rows.iter().any(
            |r| matches!(r, StorageRecord::Gap(g) if g.reason == "matcher key capacity exceeded")
        ));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn clock_rollback_never_certifies_inverted_interval() {
        let mut config = Config::example();
        config.processing_grace = Duration::from_millis(1);
        let (_arrivals_tx, arrivals_rx) = mpsc::channel(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(32);
        controls_tx
            .try_send(connected(
                0,
                Instant::now() - Duration::from_secs(2),
                SystemTime::now() + Duration::from_secs(60),
            ))
            .unwrap();
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::ZERO,
        ));
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert!(rows
            .iter()
            .any(|r| matches!(r, StorageRecord::Gap(g) if g.reason == "wall clock discontinuity")));
        assert!(!rows.iter().any(|r| matches!(r, StorageRecord::Coverage(_))));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn coverage_is_written_per_interval_and_flushed_when_health_is_lost() {
        let mut config = Config::example();
        config.window = Duration::from_millis(100);
        config.processing_grace = Duration::from_millis(10);
        let (_arrivals_tx, arrivals_rx) = mpsc::channel::<Arrival>(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(256);
        let at = Instant::now() - Duration::from_secs(1);
        let utc = SystemTime::now() - Duration::from_secs(1);
        for provider in 0..2 {
            controls_tx.try_send(connected(provider, at, utc)).unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            watch::channel(false).1,
            storage_tx,
            Duration::from_secs(60),
        ));
        let coverage = |rows: &[StorageRecord], provider: usize| {
            rows.iter()
                .filter(|r| matches!(r, StorageRecord::Coverage(c) if c.provider == provider))
                .count()
        };
        // 30 clean ticks: certified, but nothing written before the interval.
        for _ in 0..30 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert_eq!((coverage(&rows, 0), coverage(&rows, 1)), (0, 0));
        // Losing health flushes the pending certified time instead of dropping it.
        // A provider's gap invalidates every provider's comparisons, so both flush.
        controls_tx
            .try_send(StreamControl::Gap {
                provider: 1,
                stream: test_kind(),
                at: Instant::now(),
                utc: SystemTime::now(),
                reason: "test gap".into(),
            })
            .unwrap();
        tokio::task::yield_now().await;
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert_eq!((coverage(&rows, 0), coverage(&rows, 1)), (1, 1));
        // Coverage restarts after the gap; a full interval later, each writes one row.
        for _ in 0..65 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        assert_eq!((coverage(&rows, 0), coverage(&rows, 1)), (1, 1));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_writes_pending_coverage_and_exits() {
        let mut config = Config::example();
        config.window = Duration::from_millis(100);
        config.processing_grace = Duration::from_millis(10);
        let (_arrivals_tx, arrivals_rx) = mpsc::channel::<Arrival>(8);
        let (controls_tx, controls_rx) = mpsc::channel(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(256);
        let (stop_tx, stop_rx) = watch::channel(false);
        let at = Instant::now() - Duration::from_secs(1);
        let utc = SystemTime::now() - Duration::from_secs(1);
        for provider in 0..2 {
            controls_tx.try_send(connected(provider, at, utc)).unwrap();
        }
        let task = tokio::spawn(match_stream(
            test_kind(),
            2,
            config,
            arrivals_rx,
            controls_rx,
            Arc::new(InFlight::new(2)),
            Arc::default(),
            stop_rx,
            storage_tx,
            Duration::from_secs(60),
        ));
        for _ in 0..30 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        assert!(!std::iter::from_fn(|| storage_rx.try_recv().ok())
            .any(|row| matches!(row, StorageRecord::Coverage(_))));
        stop_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        let rows: Vec<_> = std::iter::from_fn(|| storage_rx.try_recv().ok()).collect();
        let mut providers: Vec<_> = rows
            .iter()
            .filter_map(|row| match row {
                StorageRecord::Coverage(coverage) => Some(coverage.provider),
                _ => None,
            })
            .collect();
        providers.sort_unstable();
        assert_eq!(providers, [0, 1]);
    }

    #[test]
    fn provider_gap_withdraws_every_providers_health() {
        let config = Config::example();
        let mut state = test_state(test_kind(), 2, &config, Arc::default());
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            state.control(connected(provider, at, utc));
            state.coverage[provider].as_mut().unwrap().certified = Some(utc);
        }
        let rows = state.control(StreamControl::Gap {
            provider: 0,
            stream: test_kind(),
            at,
            utc,
            reason: "arrival queue full".into(),
        });
        let mut providers: Vec<_> = rows
            .iter()
            .filter_map(|row| match row {
                StorageRecord::Coverage(coverage) => Some(coverage.provider),
                _ => None,
            })
            .collect();
        providers.sort_unstable();
        assert_eq!(providers, [0, 1]);
        assert!(state.coverage.iter().all(Option::is_none));
    }

    #[test]
    fn coverage_starts_after_connection_warmup() {
        let mut config = Config::example();
        config.connection_warmup = Duration::from_millis(1500);
        let mut state = test_state(test_kind(), 1, &config, Arc::default());
        let (at, utc) = (Instant::now(), SystemTime::now());
        state.control(connected(0, at, utc));
        let coverage = state.coverage[0].expect("coverage started");
        assert_eq!(coverage.start, at + config.connection_warmup);
        assert_eq!(coverage.start_utc, utc + config.connection_warmup);
        assert_eq!(
            state.matcher.eligible_since(0),
            Some(at + config.connection_warmup)
        );
    }

    #[test]
    fn processed_misses_wait_for_the_slot_stream_to_confirm_or_fork() {
        let mut config = Config::example();
        config.connection_warmup = Duration::ZERO;
        let fates = Arc::new(SlotFates::default());
        let kind = |data_type| StreamKind {
            data_type,
            commitment: Commitment::Processed,
        };
        let mut txs = test_state(kind(DataType::Transaction), 2, &config, fates.clone());
        let mut slots = test_state(kind(DataType::Slot), 2, &config, fates);
        let (at, utc) = (Instant::now(), SystemTime::now());
        for provider in 0..2 {
            txs.matcher
                .connection_change(provider, at, utc, true, "subscribed");
        }
        let delivered = |slot: u64| Arrival {
            message_bytes: Some(100),
            provider: 0,
            key: EventKey::Transaction {
                slot,
                signature: vec![slot as u8],
            },
            at: at + Duration::from_millis(1),
            utc,
        };
        txs.arrival(delivered(10), config.processing_grace);
        txs.arrival(delivered(11), config.processing_grace);
        let outcomes = |rows: Vec<StorageRecord>| -> Vec<(u64, usize, Outcome)> {
            rows.into_iter()
                .filter_map(|row| match row {
                    StorageRecord::Lag(lag) => Some((lag.key.slot(), lag.provider, lag.outcome)),
                    _ => None,
                })
                .collect()
        };
        // Provider 1 never delivered: only the on-time rows are written at close.
        let end = at + config.window + Duration::from_secs(1);
        let mut rows = outcomes(txs.close(end, end));
        rows.sort_unstable_by_key(|row| row.0);
        assert_eq!(rows, [(10, 0, Outcome::OnTime), (11, 0, Outcome::OnTime)]);
        let slot = |slot, status: SlotStatus| Arrival {
            message_bytes: None,
            provider: 1,
            key: EventKey::Slot {
                slot,
                status: status as i32,
            },
            at: end,
            utc,
        };
        slots.arrival(slot(10, SlotStatus::SlotConfirmed), Duration::MAX);
        slots.arrival(slot(11, SlotStatus::SlotDead), Duration::MAX);
        let mut rows = outcomes(txs.close(end, end));
        rows.sort_unstable_by_key(|row| row.0);
        assert_eq!(rows, [(10, 1, Outcome::Missed), (11, 1, Outcome::Forked)]);
    }
}
