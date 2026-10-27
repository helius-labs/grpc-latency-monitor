//! Shared contracts between stream readers, the matcher and ClickHouse writer.
//! All durations use monotonic time. UTC is for reporting only.

use std::time::SystemTime;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Account,
    Transaction,
    Slot,
}

impl DataType {
    pub const ALL: [Self; 3] = [Self::Account, Self::Transaction, Self::Slot];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Transaction => "transaction",
            Self::Slot => "slot",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Commitment {
    Processed,
    Finalized,
}

impl Commitment {
    pub const ALL: [Self; 2] = [Self::Processed, Self::Finalized];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamKind {
    pub data_type: DataType,
    pub commitment: Commitment,
}

impl StreamKind {
    /// Slot lifecycle notifications contain all statuses regardless of the
    /// requested commitment. Keep one canonical stream under `processed`.
    pub const ALL: [Self; 5] = [
        Self {
            data_type: DataType::Account,
            commitment: Commitment::Processed,
        },
        Self {
            data_type: DataType::Account,
            commitment: Commitment::Finalized,
        },
        Self {
            data_type: DataType::Transaction,
            commitment: Commitment::Processed,
        },
        Self {
            data_type: DataType::Transaction,
            commitment: Commitment::Finalized,
        },
        Self {
            data_type: DataType::Slot,
            commitment: Commitment::Processed,
        },
    ];

    pub const fn index(self) -> Option<usize> {
        match (self.data_type, self.commitment) {
            (DataType::Account, Commitment::Processed) => Some(0),
            (DataType::Account, Commitment::Finalized) => Some(1),
            (DataType::Transaction, Commitment::Processed) => Some(2),
            (DataType::Transaction, Commitment::Finalized) => Some(3),
            (DataType::Slot, Commitment::Processed) => Some(4),
            (DataType::Slot, Commitment::Finalized) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventKey {
    /// One account update per transaction in a slot. Repeated deliveries of the
    /// same tuple are deduplicated; node-local write_version is not an identity.
    Account {
        slot: u64,
        pubkey: Vec<u8>,
        txn_signature: Vec<u8>,
    },
    Transaction {
        slot: u64,
        signature: Vec<u8>,
    },
    Slot {
        slot: u64,
        status: i32,
    },
}

impl EventKey {
    pub const fn slot(&self) -> u64 {
        match self {
            Self::Account { slot, .. }
            | Self::Transaction { slot, .. }
            | Self::Slot { slot, .. } => *slot,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Arrival {
    /// Encoded SubscribeUpdate size for account/transaction messages; slots are unmeasured.
    pub message_bytes: Option<u64>,
    pub provider: usize,
    pub key: EventKey,
    pub at: Instant,
    pub utc: SystemTime,
}

/// Per provider and event. There is one window: a delivery that is not on time
/// counts as a missed deadline (`Missed`), even if it arrives later. A processed
/// miss in a slot that never confirmed is `Forked` instead, and is not a missed deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    OnTime,
    Missed,
    Forked,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OnTime => "on_time",
            Self::Missed => "missed",
            Self::Forked => "forked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LagRecord {
    /// Provider-specific message size; NULL for missed and forked events, slots and old data.
    pub message_bytes: Option<u64>,
    pub stream: StreamKind,
    pub key: EventKey,
    pub first_utc: SystemTime,
    pub provider: usize,
    pub outcome: Outcome,
    pub lag_ns: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ConnectionRecord {
    pub stream: StreamKind,
    pub provider: usize,
    pub utc: SystemTime,
    pub connected: bool,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct GapRecord {
    pub stream: StreamKind,
    pub provider: Option<usize>,
    pub utc: SystemTime,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct CoverageRecord {
    pub stream: StreamKind,
    pub provider: usize,
    pub start: SystemTime,
    pub end: SystemTime,
}

#[derive(Debug, Clone)]
pub enum StorageRecord {
    Lag(LagRecord),
    Connection(ConnectionRecord),
    Gap(GapRecord),
    Coverage(CoverageRecord),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_streams_have_one_slot_subscription_and_consistent_storage_indices() {
        assert_eq!(StreamKind::ALL.len(), 5);
        assert_eq!(
            StreamKind::ALL
                .iter()
                .filter(|s| s.data_type == DataType::Slot)
                .count(),
            1
        );
        assert_eq!(
            StreamKind::ALL
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            5
        );
        for (index, stream) in StreamKind::ALL.into_iter().enumerate() {
            assert_eq!(stream.index(), Some(index));
        }
        assert_eq!(
            StreamKind {
                data_type: DataType::Slot,
                commitment: Commitment::Finalized
            }
            .index(),
            None
        );
    }
}
