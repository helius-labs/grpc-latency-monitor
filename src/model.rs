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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventKey {
    /// The first delivery of an account in a slot; subsequent writes are not
    /// individually compared because write_version is not cross-provider stable.
    Account {
        slot: u64,
        pubkey: Vec<u8>,
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
