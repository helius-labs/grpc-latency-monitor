//! A processed account update or transaction on a fork that never confirms is
//! delivered only by providers that replayed that fork. Counting every other
//! provider as missing its deadline would measure fork exposure, not reliability. A
//! processed miss waits for its slot's fate: in a confirmed slot it is a missed
//! deadline (`missed`); in a dead or never-confirmed slot it is recorded as `forked`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
    time::Duration,
};

use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::SlotStatus;

use crate::model::{LagRecord, Outcome, StorageRecord};

/// Slots remembered below the RPC's finalized reference (about an hour at 400 ms).
/// A pending miss resolves long before its slot falls out of this range.
const RETAINED_SLOTS: u64 = 10_000;

/// Slot statuses reported by any provider on the slot streams.
#[derive(Default)]
pub struct SlotFates {
    inner: Mutex<Fates>,
}

#[derive(Default)]
struct Fates {
    confirmed: BTreeSet<u64>,
    dead: BTreeSet<u64>,
    finalized: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    Confirmed,
    Forked,
    Unknown,
}

impl SlotFates {
    pub fn record(&self, slot: u64, status: i32) {
        let mut fates = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match SlotStatus::try_from(status) {
            Ok(SlotStatus::SlotConfirmed) => {
                fates.confirmed.insert(slot);
            }
            Ok(SlotStatus::SlotFinalized) => {
                fates.confirmed.insert(slot);
            }
            Ok(SlotStatus::SlotDead) => {
                fates.dead.insert(slot);
            }
            _ => {}
        }
    }

    /// Only the independent RPC reference can advance the pruning watermark.
    pub fn prune(&self, finalized: u64) {
        let mut fates = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if fates.finalized.is_none_or(|old| finalized > old) {
            fates.finalized = Some(finalized);
            let floor = finalized.saturating_sub(RETAINED_SLOTS);
            fates.confirmed = fates.confirmed.split_off(&floor);
            fates.dead = fates.dead.split_off(&floor);
        }
    }

    /// A later finalization cannot decide an earlier slot: independent slot
    /// workers can process it before that slot's queued confirmation. Undecided
    /// slots remain pending until confirmation, an explicit dead status, or the
    /// filter's bounded timeout. The finalized watermark only bounds retention.
    pub fn fate(&self, slot: u64) -> Fate {
        let fates = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if fates.confirmed.contains(&slot) {
            Fate::Confirmed
        } else if fates.dead.contains(&slot) {
            Fate::Forked
        } else {
            Fate::Unknown
        }
    }
}

struct Pending {
    since: Instant,
    rows: Vec<LagRecord>,
}

/// Holds misses of one processed stream until their slot's fate is known.
pub struct ForkFilter {
    pending: BTreeMap<u64, Pending>,
    rows: usize,
    timeout: Duration,
    capacity: usize,
}

impl ForkFilter {
    pub fn new(timeout: Duration, capacity: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            rows: 0,
            timeout,
            capacity,
        }
    }

    /// Pass rows through, holding misses whose slot is undecided, and release held
    /// misses that are now decided. A slot undecided after `timeout` (for example
    /// while every slot stream is down) counts as forked, not a missed deadline; so do the
    /// oldest held misses beyond `capacity`.
    pub fn filter(
        &mut self,
        rows: Vec<StorageRecord>,
        now: Instant,
        fates: &SlotFates,
    ) -> Vec<StorageRecord> {
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            match row {
                StorageRecord::Lag(lag) if lag.outcome == Outcome::Missed => {
                    let slot = lag.key.slot();
                    match fates.fate(slot) {
                        Fate::Confirmed => out.push(StorageRecord::Lag(lag)),
                        Fate::Forked => out.push(forked(lag)),
                        Fate::Unknown => {
                            self.pending
                                .entry(slot)
                                .or_insert_with(|| Pending {
                                    since: now,
                                    rows: Vec::new(),
                                })
                                .rows
                                .push(lag);
                            self.rows += 1;
                        }
                    }
                }
                row => out.push(row),
            }
        }
        let timeout = self.timeout;
        self.pending.retain(|slot, pending| {
            let fate = match fates.fate(*slot) {
                Fate::Unknown if now.duration_since(pending.since) >= timeout => Fate::Forked,
                fate => fate,
            };
            if fate == Fate::Unknown {
                return true;
            }
            self.rows -= pending.rows.len();
            out.extend(pending.rows.drain(..).map(|lag| match fate {
                Fate::Confirmed => StorageRecord::Lag(lag),
                _ => forked(lag),
            }));
            false
        });
        while self.rows > self.capacity {
            let Some((_, pending)) = self.pending.pop_first() else {
                break;
            };
            self.rows -= pending.rows.len();
            out.extend(pending.rows.into_iter().map(forked));
        }
        out
    }

    /// Release every held miss as forked: its slot's fate is no longer awaited.
    pub fn drain(&mut self) -> Vec<StorageRecord> {
        self.rows = 0;
        std::mem::take(&mut self.pending)
            .into_values()
            .flat_map(|pending| pending.rows)
            .map(forked)
            .collect()
    }
}

fn forked(lag: LagRecord) -> StorageRecord {
    StorageRecord::Lag(LagRecord {
        outcome: Outcome::Forked,
        ..lag
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Commitment, DataType, EventKey, StreamKind};
    use std::time::SystemTime;

    fn lag(slot: u64, provider: usize, outcome: Outcome) -> StorageRecord {
        StorageRecord::Lag(LagRecord {
            message_bytes: None,
            stream: StreamKind {
                data_type: DataType::Transaction,
                commitment: Commitment::Processed,
            },
            key: EventKey::Transaction {
                slot,
                signature: vec![slot as u8],
            },
            first_utc: SystemTime::UNIX_EPOCH,
            provider,
            outcome,
            lag_ns: (outcome == Outcome::OnTime).then_some(0),
        })
    }

    fn outcomes(rows: &[StorageRecord]) -> Vec<(u64, usize, Outcome)> {
        rows.iter()
            .map(|row| match row {
                StorageRecord::Lag(lag) => (lag.key.slot(), lag.provider, lag.outcome),
                _ => panic!("unexpected row"),
            })
            .collect()
    }

    const CONFIRMED: i32 = SlotStatus::SlotConfirmed as i32;
    const FINALIZED: i32 = SlotStatus::SlotFinalized as i32;
    const DEAD: i32 = SlotStatus::SlotDead as i32;

    #[test]
    fn misses_wait_for_confirmation_and_on_time_rows_pass() {
        let fates = SlotFates::default();
        let mut filter = ForkFilter::new(Duration::from_secs(30), 100);
        let t = Instant::now();
        let rows = filter.filter(
            vec![lag(10, 0, Outcome::OnTime), lag(10, 1, Outcome::Missed)],
            t,
            &fates,
        );
        assert_eq!(outcomes(&rows), [(10, 0, Outcome::OnTime)]);
        assert!(filter
            .filter(vec![], t + Duration::from_secs(1), &fates)
            .is_empty());
        fates.record(10, CONFIRMED);
        let rows = filter.filter(vec![], t + Duration::from_secs(2), &fates);
        assert_eq!(outcomes(&rows), [(10, 1, Outcome::Missed)]);
        // Already decided: released immediately.
        let rows = filter.filter(vec![lag(10, 1, Outcome::Missed)], t, &fates);
        assert_eq!(outcomes(&rows), [(10, 1, Outcome::Missed)]);
    }

    #[test]
    fn finalization_does_not_overtake_a_queued_confirmation() {
        let fates = SlotFates::default();
        let mut filter = ForkFilter::new(Duration::from_secs(30), 100);
        let t = Instant::now();
        let held = vec![
            lag(10, 1, Outcome::Missed),
            lag(11, 1, Outcome::Missed),
            lag(12, 1, Outcome::Missed),
            lag(13, 1, Outcome::Missed),
        ];
        assert!(filter.filter(held, t, &fates).is_empty());
        fates.record(11, DEAD);
        fates.record(12, FINALIZED);
        // Another worker's finalization must not resolve slot 10 prematurely.
        let rows = filter.filter(vec![], t, &fates);
        assert_eq!(
            outcomes(&rows),
            [(11, 1, Outcome::Forked), (12, 1, Outcome::Missed),]
        );
        fates.record(10, CONFIRMED);
        let rows = filter.filter(vec![], t + Duration::from_secs(1), &fates);
        assert_eq!(outcomes(&rows), [(10, 1, Outcome::Missed)]);
        let rows = filter.filter(vec![], t + Duration::from_secs(30), &fates);
        assert_eq!(outcomes(&rows), [(13, 1, Outcome::Forked)]);
    }

    #[test]
    fn draining_releases_held_misses_as_forked() {
        let fates = SlotFates::default();
        let mut filter = ForkFilter::new(Duration::from_secs(30), 100);
        let t = Instant::now();
        let held = vec![lag(10, 1, Outcome::Missed), lag(11, 2, Outcome::Missed)];
        assert!(filter.filter(held, t, &fates).is_empty());
        assert_eq!(
            outcomes(&filter.drain()),
            [(10, 1, Outcome::Forked), (11, 2, Outcome::Forked)]
        );
        fates.record(10, CONFIRMED);
        assert!(filter.filter(vec![], t, &fates).is_empty());
    }

    #[test]
    fn oldest_misses_beyond_capacity_are_forked() {
        let fates = SlotFates::default();
        let mut filter = ForkFilter::new(Duration::from_secs(30), 2);
        let t = Instant::now();
        let rows = filter.filter(
            vec![
                lag(10, 1, Outcome::Missed),
                lag(11, 1, Outcome::Missed),
                lag(11, 2, Outcome::Missed),
            ],
            t,
            &fates,
        );
        assert_eq!(outcomes(&rows), [(10, 1, Outcome::Forked)]);
    }

    #[test]
    fn fates_below_the_retained_range_are_forgotten() {
        let fates = SlotFates::default();
        fates.record(5, CONFIRMED);
        fates.record(RETAINED_SLOTS + 10, FINALIZED);
        assert_eq!(
            fates.fate(5),
            Fate::Confirmed,
            "provider finalization cannot prune"
        );
        fates.prune(RETAINED_SLOTS + 10);
        assert_eq!(fates.fate(5), Fate::Unknown);
        assert_eq!(fates.fate(RETAINED_SLOTS + 10), Fate::Confirmed);
        assert_eq!(fates.fate(RETAINED_SLOTS + 11), Fate::Unknown);
    }
}
