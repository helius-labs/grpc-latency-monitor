//! Bounded ClickHouse sink. A successful return means every received row was acknowledged
//! by ClickHouse; a failure is terminal and must be surfaced to the operator. This is not
//! durable spooling: process loss, producer drops, and ambiguous HTTP insert failures make
//! coverage unknown. Never infer a healthy interval merely from rows being present.

use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use clickhouse::{Client, Compression, Row, RowOwned, RowWrite};
use serde::Serialize;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::config::SubscriberConfig;
use crate::model::{self, DataType, EventKey, StorageRecord, StreamKind};

/// Durable minute IDs, not a row spool. Keep the directory on persistent storage.
/// A failed database intent (including an ambiguous response) must survive restart
/// too, so this file is synced before any insert and cleared only after all acks.
pub struct WriteJournal {
    file: std::fs::File,
    pending: Vec<u32>,
}

impl WriteJournal {
    pub async fn open(directory: &std::path::Path, identity: &[&str]) -> anyhow::Result<Self> {
        let mut hash = blake3::Hasher::new();
        for field in identity {
            hash.update(&(field.len() as u64).to_le_bytes());
            hash.update(field.as_bytes());
        }
        let directory = directory.to_path_buf();
        let key = hash.finalize().to_hex();
        let (file, pending) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            use std::io::Read;
            std::fs::create_dir_all(&directory)?;
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(directory.join(format!("{key}.pending")))?;
            file.try_lock()
                .context("another writer owns this recovery journal")?;
            // Persist the directory entry as well as later file contents.
            #[cfg(unix)]
            std::fs::File::open(&directory)?.sync_all()?;
            let mut text = String::new();
            file.read_to_string(&mut text)?;
            let pending = text
                .lines()
                .map(str::parse)
                .collect::<Result<Vec<u32>, _>>()
                .context(
                    "invalid pending-write journal; preserve it and investigate before restarting",
                )?;
            Ok((file, pending))
        })
        .await??;
        Ok(Self { file, pending })
    }

    async fn record(&mut self, minutes: Vec<u32>) -> anyhow::Result<()> {
        let text: String = minutes.iter().map(|minute| format!("{minute}\n")).collect();
        let mut file = self.file.try_clone()?;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::{Seek, Write};
            file.rewind()?;
            file.write_all(text.as_bytes())?;
            file.set_len(text.len() as u64)?;
            file.sync_all()
        })
        .await??;
        self.pending = minutes;
        Ok(())
    }

    pub async fn recover(
        &mut self,
        client: &Client,
        subscriber: &SubscriberConfig,
    ) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        tracing::warn!(minutes = self.pending.len(), region = %subscriber.region,
            cloud_provider = %subscriber.cloud_provider,
            "Recovering unacknowledged raw minutes as unknown");
        let rows: Vec<_> = self
            .pending
            .iter()
            .map(|&bucket| IntentRow {
                region: subscriber.region.clone(),
                cloud_provider: subscriber.cloud_provider.clone(),
                bucket,
                expected: 0,
                incomplete: 1,
            })
            .collect();
        insert_rows(client, "benchmark_dirty_minutes", &rows)
            .await
            .context("marking unacknowledged write minutes unknown before restarting")?;
        self.record(Vec::new()).await
    }
}

/// Ordered account/processed, account/finalized, transaction/processed,
/// transaction/finalized, slot/processed, slot/finalized.
/// `filters` is the normalized subscription request/filter description used to form `id`.
#[derive(Debug, Clone)]
pub struct SubscriptionVersion {
    pub id: String,
    pub filters: String,
}

#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WriteSummary {
    pub records: u64,
    pub batches: u64,
}

/// The count includes all rows in the failed batch, including rows that may have
/// reached ClickHouse before an ambiguous HTTP error. Do not retry blindly: ClickHouse
/// MergeTree does not enforce uniqueness. The caller must mark coverage unknown.
#[derive(Debug)]
pub struct WriteError {
    pub message: String,
    pub unacknowledged_records: usize,
    pub first_unacknowledged_utc: Option<SystemTime>,
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} unacknowledged records; coverage unknown)",
            self.message, self.unacknowledged_records
        )
    }
}
impl std::error::Error for WriteError {}

#[derive(Row, Serialize, Debug)]
struct LagRow {
    event_id: u128,
    slot: u64,
    data_type: String,
    commitment: String,
    cloud_provider: String,
    region: String,
    subscription_id: String,
    provider: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    event_utc: OffsetDateTime,
    outcome: String,
    lag_ns: Option<u64>,
    message_bytes: Option<u64>,
}

#[derive(Row, Serialize, Debug)]
struct ConnectionRow {
    cloud_provider: String,
    region: String,
    subscription_id: String,
    provider: String,
    data_type: String,
    commitment: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    event_utc: OffsetDateTime,
    connected: bool,
    reason: String,
}

#[derive(Row, Serialize, Debug)]
struct VersionRow {
    subscription_id: String,
    data_type: String,
    commitment: String,
    filters: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    registered_utc: OffsetDateTime,
}

/// Acknowledged before any raw insert. Unmatched counts permanently withhold a
/// minute after a rejected or partially accepted insert; no retry can certify it.
#[derive(Row, Serialize, Debug)]
struct IntentRow {
    region: String,
    cloud_provider: String,
    bucket: u32,
    expected: u64,
    incomplete: u64,
}

fn write_intents(ctx: &Context, lags: &[LagRow]) -> Result<Vec<IntentRow>, String> {
    let mut counts = std::collections::BTreeMap::new();
    for lag in lags {
        let bucket = u32::try_from(lag.event_utc.unix_timestamp().div_euclid(60) * 60)
            .map_err(|_| "event minute is outside ClickHouse DateTime range".to_string())?;
        *counts.entry(bucket).or_insert(0_u64) += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(bucket, expected)| IntentRow {
            region: ctx.region.clone(),
            cloud_provider: ctx.cloud_provider.clone(),
            bucket,
            expected,
            incomplete: 0,
        })
        .collect())
}

#[derive(Row, Serialize, Debug)]
struct SubscriberRow {
    region: String,
    cloud_provider: String,
    latitude: f64,
    longitude: f64,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    registered_utc: OffsetDateTime,
}

impl SubscriberRow {
    fn new(subscriber: &SubscriberConfig, registered_utc: SystemTime) -> Self {
        Self {
            region: subscriber.region.clone(),
            cloud_provider: subscriber.cloud_provider.clone(),
            latitude: subscriber.latitude,
            longitude: subscriber.longitude,
            registered_utc: utc(registered_utc),
        }
    }
}

/// Register location once per startup instead of repeating coordinates in fact rows.
pub async fn register_subscriber(
    client: &Client,
    subscriber: &SubscriberConfig,
) -> anyhow::Result<()> {
    subscriber.validate()?;
    insert_rows(
        client,
        "benchmark_subscriber_locations",
        &[SubscriberRow::new(subscriber, SystemTime::now())],
    )
    .await
    .context("registering subscriber location in ClickHouse")
}

#[derive(Row, Serialize, Debug)]
struct HealthRow {
    cloud_provider: String,
    region: String,
    subscription_id: String,
    provider: Option<String>,
    data_type: String,
    commitment: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    interval_start: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos::option")]
    interval_end: Option<OffsetDateTime>,
    status: String,
    reason: String,
}

/// Check table/column availability and insertability before opening provider streams.
/// Type compatibility is checked by the driver when actual rows are serialized.
pub async fn validate_schema(client: &Client) -> anyhow::Result<()> {
    // Metadata is small; avoid compression negotiation and require validation even
    // if the caller disabled it. Clones share the driver's metadata cache.
    let client = client
        .clone()
        .with_compression(Compression::None)
        .with_validation(true);
    tokio::time::timeout(Duration::from_secs(30), async {
        check_row::<LagRow>(&client, "benchmark_event_lags").await?;
        check_row::<ConnectionRow>(&client, "benchmark_connection_events").await?;
        check_row::<VersionRow>(&client, "benchmark_subscription_versions").await?;
        check_row::<HealthRow>(&client, "benchmark_measurement_health").await?;
        check_row::<IntentRow>(&client, "benchmark_dirty_minutes").await?;
        check_row::<SubscriberRow>(&client, "benchmark_subscriber_locations").await
    })
    .await
    .context(
        "ClickHouse startup schema check timed out; check connectivity and metadata permissions",
    )?
}

async fn check_row<T: Row>(client: &Client, table: &str) -> anyhow::Result<()> {
    // Preparing an insert only runs DESCRIBE TABLE and checks the Row's derived
    // column names against metadata. No INSERT request starts until write/end.
    // Drop the handle without either call: this check never writes data.
    let insert = client.insert::<T>(table).await.with_context(|| {
        format!(
            "ClickHouse startup schema check failed for {table}; apply Goose migrations \
             to the configured database and check metadata permissions; see sql/README.md"
        )
    })?;
    drop(insert);
    Ok(())
}

/// Domain-separated canonical key encoding; neither subscriber identity nor provider is part of
/// the identity. Accounts include the modifying transaction's signature, so writes
/// to the same pubkey by different transactions in one slot remain distinct.
/// The stored `UInt128` event ID: the first 16 bytes of [`event_id`]. 128 bits keep
/// collisions negligible while taking a quarter of the 64-character hex encoding.
pub fn stored_event_id(stream: StreamKind, key: &EventKey) -> u128 {
    let hash = event_id(stream, key);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

pub fn event_id(stream: StreamKind, key: &EventKey) -> blake3::Hash {
    let mut h = blake3::Hasher::new();
    h.update(b"solana-streaming-event-v1\0");
    h.update(stream.data_type.as_str().as_bytes());
    h.update(&[0]);
    h.update(stream.commitment.as_str().as_bytes());
    h.update(&[0]);
    h.update(&key.slot().to_be_bytes());
    match key {
        EventKey::Account {
            pubkey,
            txn_signature,
            ..
        } => {
            h.update(b"account-transaction-v2\0");
            h.update(&(pubkey.len() as u64).to_be_bytes());
            h.update(pubkey);
            h.update(&(txn_signature.len() as u64).to_be_bytes());
            h.update(txn_signature);
        }
        EventKey::Transaction { signature, .. } => {
            h.update(b"transaction\0");
            h.update(&(signature.len() as u64).to_be_bytes());
            h.update(signature);
        }
        EventKey::Slot { status, .. } => {
            h.update(b"slot\0");
            h.update(&status.to_be_bytes());
        }
    }
    h.finalize()
}

fn utc(value: SystemTime) -> OffsetDateTime {
    value.into()
}

struct Context {
    cloud_provider: String,
    region: String,
    providers: Vec<String>,
    versions: [SubscriptionVersion; StreamKind::ALL.len()],
}

impl Context {
    fn provider(&self, index: usize) -> Result<String, String> {
        self.providers
            .get(index)
            .cloned()
            .ok_or_else(|| format!("unknown provider index {index}"))
    }
    fn subscription(&self, stream: StreamKind) -> Result<String, String> {
        stream
            .index()
            .map(|index| self.versions[index].id.clone())
            .ok_or_else(|| {
                "inactive stream: slots use a single subscription stored as processed".into()
            })
    }
    fn lag(&self, record: &model::LagRecord) -> Result<LagRow, String> {
        if record.stream.data_type
            != match record.key {
                EventKey::Account { .. } => DataType::Account,
                EventKey::Transaction { .. } => DataType::Transaction,
                EventKey::Slot { .. } => DataType::Slot,
            }
        {
            return Err("event key does not match stream type".into());
        }
        if (record.outcome == model::Outcome::OnTime) != record.lag_ns.is_some() {
            return Err("on-time requires lag; missed and forked require NULL lag".into());
        }
        Ok(LagRow {
            event_id: stored_event_id(record.stream, &record.key),
            slot: record.key.slot(),
            data_type: record.stream.data_type.as_str().into(),
            commitment: record.stream.commitment.as_str().into(),
            cloud_provider: self.cloud_provider.clone(),
            region: self.region.clone(),
            subscription_id: self.subscription(record.stream)?,
            provider: self.provider(record.provider)?,
            event_utc: utc(record.first_utc),
            outcome: record.outcome.as_str().into(),
            lag_ns: record.lag_ns,
            message_bytes: record.message_bytes,
        })
    }
    fn connection(&self, record: &model::ConnectionRecord) -> Result<ConnectionRow, String> {
        Ok(ConnectionRow {
            cloud_provider: self.cloud_provider.clone(),
            region: self.region.clone(),
            subscription_id: self.subscription(record.stream)?,
            provider: self.provider(record.provider)?,
            data_type: record.stream.data_type.as_str().into(),
            commitment: record.stream.commitment.as_str().into(),
            event_utc: utc(record.utc),
            connected: record.connected,
            reason: record.reason.clone(),
        })
    }
    fn coverage(&self, record: &model::CoverageRecord) -> Result<HealthRow, String> {
        Ok(HealthRow {
            cloud_provider: self.cloud_provider.clone(),
            region: self.region.clone(),
            subscription_id: self.subscription(record.stream)?,
            provider: Some(self.provider(record.provider)?),
            data_type: record.stream.data_type.as_str().into(),
            commitment: record.stream.commitment.as_str().into(),
            interval_start: utc(record.start),
            interval_end: Some(utc(record.end)),
            status: "healthy".into(),
            reason: "certified interval".into(),
        })
    }
    fn gap(&self, record: &model::GapRecord) -> Result<HealthRow, String> {
        Ok(HealthRow {
            cloud_provider: self.cloud_provider.clone(),
            region: self.region.clone(),
            subscription_id: self.subscription(record.stream)?,
            provider: record
                .provider
                .map(|index| self.provider(index))
                .transpose()?,
            data_type: record.stream.data_type.as_str().into(),
            commitment: record.stream.commitment.as_str().into(),
            interval_start: utc(record.utc),
            interval_end: Some(utc(record.utc)),
            status: "unknown".into(),
            reason: record.reason.clone(),
        })
    }
}

fn record_utc(record: &StorageRecord) -> SystemTime {
    match record {
        StorageRecord::Lag(row) => row.first_utc,
        StorageRecord::Connection(row) => row.utc,
        StorageRecord::Gap(row) => row.utc,
        StorageRecord::Coverage(row) => row.start,
    }
}

const INSERT_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const INSERT_END_TIMEOUT: Duration = Duration::from_secs(30);

async fn insert_rows<T: RowOwned + RowWrite>(
    client: &Client,
    table: &str,
    rows: &[T],
) -> clickhouse::error::Result<()> {
    insert_rows_with_timeouts(client, table, rows, INSERT_SEND_TIMEOUT, INSERT_END_TIMEOUT).await
}

async fn insert_rows_with_timeouts<T: RowOwned + RowWrite>(
    client: &Client,
    table: &str,
    rows: &[T],
    send_timeout: Duration,
    end_timeout: Duration,
) -> clickhouse::error::Result<()> {
    if !rows.is_empty() {
        let mut insert = tokio::time::timeout(end_timeout, client.insert::<T>(table))
            .await
            .map_err(|_| clickhouse::error::Error::TimedOut)??
            .with_timeouts(Some(send_timeout), Some(end_timeout));
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    Ok(())
}

async fn flush(
    client: &Client,
    journal: &mut WriteJournal,
    ctx: &Context,
    pending: &mut Vec<StorageRecord>,
    summary: &mut WriteSummary,
) -> Result<(), WriteError> {
    if pending.is_empty() {
        return Ok(());
    }
    let failure = |message: String| WriteError {
        message,
        unacknowledged_records: pending.len(),
        first_unacknowledged_utc: pending.iter().map(record_utc).min(),
    };
    let mut lags = Vec::new();
    let mut connections = Vec::new();
    let mut gaps = Vec::new();
    for record in pending.iter() {
        match record {
            StorageRecord::Lag(row) => lags.push(ctx.lag(row).map_err(failure)?),
            StorageRecord::Connection(row) => {
                connections.push(ctx.connection(row).map_err(failure)?)
            }
            StorageRecord::Gap(row) => gaps.push(ctx.gap(row).map_err(failure)?),
            StorageRecord::Coverage(row) => gaps.push(ctx.coverage(row).map_err(failure)?),
        }
    }
    let intents = write_intents(ctx, &lags).map_err(failure)?;
    if !intents.is_empty() {
        journal
            .record(intents.iter().map(|row| row.bucket).collect())
            .await
            .map_err(|e| failure(format!("recording pending raw minutes failed: {e:#}")))?;
    }
    insert_rows(client, "benchmark_dirty_minutes", &intents)
        .await
        .map_err(|e| failure(format!("raw write intent failed: {e}")))?;
    insert_rows(client, "benchmark_event_lags", &lags)
        .await
        .map_err(|e| failure(format!("benchmark_event_lags insert failed: {e}")))?;
    insert_rows(client, "benchmark_connection_events", &connections)
        .await
        .map_err(|e| failure(format!("benchmark_connection_events insert failed: {e}")))?;
    insert_rows(client, "benchmark_measurement_health", &gaps)
        .await
        .map_err(|e| failure(format!("benchmark_measurement_health insert failed: {e}")))?;
    if !intents.is_empty() {
        journal
            .record(Vec::new())
            .await
            .map_err(|e| failure(format!("clearing acknowledged raw minutes failed: {e:#}")))?;
    }
    summary.records += pending.len() as u64;
    summary.batches += 1;
    pending.clear();
    Ok(())
}

/// Consumes a bounded Tokio receiver. The caller must send Gap records on producer drops;
/// a closed channel is not itself evidence of healthy coverage. On failure, this function
/// stops consuming immediately and returns an error including the unacknowledged batch.
/// The caller must treat the remaining receiver contents and any interval after failure
/// as unknown. Use bounded retry/restart supervision outside this sink, not silent retries.
#[allow(clippy::too_many_arguments)]
pub async fn write(
    client: Client,
    mut journal: WriteJournal,
    cloud_provider: String,
    region: String,
    providers: Vec<String>,
    versions: [SubscriptionVersion; StreamKind::ALL.len()],
    mut receiver: mpsc::Receiver<StorageRecord>,
    limits: BatchLimits,
) -> Result<WriteSummary, WriteError> {
    let invalid = |message: &str| WriteError {
        message: message.into(),
        unacknowledged_records: 0,
        first_unacknowledged_utc: None,
    };
    if !journal.pending.is_empty() {
        return Err(invalid(
            "pending-write recovery must complete before ingestion",
        ));
    }
    if cloud_provider.is_empty()
        || region.is_empty()
        || providers.is_empty()
        || providers.iter().any(String::is_empty)
        || versions
            .iter()
            .any(|v| v.id.is_empty() || v.filters.is_empty())
        || limits.max_records == 0
        || limits.flush_interval.is_zero()
    {
        return Err(invalid(
            "invalid sink configuration: names, IDs, filters and batch limits must be nonempty",
        ));
    }
    let mut names = providers.clone();
    names.sort();
    names.dedup();
    if names.len() != providers.len() {
        return Err(invalid("duplicate provider name"));
    }
    let mut ids: Vec<_> = versions.iter().map(|v| &v.id).collect();
    ids.sort();
    ids.dedup();
    if ids.len() != StreamKind::ALL.len() {
        return Err(invalid("subscription IDs must be distinct"));
    }
    let ctx = Context {
        cloud_provider,
        region,
        providers,
        versions,
    };
    let mut metadata = Vec::new();
    let registered_utc = utc(SystemTime::now());
    for (stream, version) in StreamKind::ALL.into_iter().zip(&ctx.versions) {
        metadata.push(VersionRow {
            subscription_id: version.id.clone(),
            data_type: stream.data_type.as_str().into(),
            commitment: stream.commitment.as_str().into(),
            filters: version.filters.clone(),
            registered_utc,
        });
    }
    insert_rows(&client, "benchmark_subscription_versions", &metadata)
        .await
        .map_err(|e| {
            invalid(&format!(
                "benchmark_subscription_versions insert failed: {e}; coverage unknown"
            ))
        })?;
    let mut pending = Vec::with_capacity(limits.max_records);
    let mut summary = WriteSummary::default();
    let timer = tokio::time::sleep(limits.flush_interval);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            record = receiver.recv() => match record {
                Some(record) => {
                    pending.push(record);
                    if pending.len() == limits.max_records {
                        flush(&client, &mut journal, &ctx, &mut pending, &mut summary).await?;
                        timer.as_mut().reset(tokio::time::Instant::now() + limits.flush_interval);
                    }
                }
                None => {
                    flush(&client, &mut journal, &ctx, &mut pending, &mut summary).await?;
                    return Ok(summary);
                }
            },
            _ = &mut timer => {
                flush(&client, &mut journal, &ctx, &mut pending, &mut summary).await?;
                timer.as_mut().reset(tokio::time::Instant::now() + limits.flush_interval);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Commitment;

    #[tokio::test]
    async fn pending_minutes_survive_restart_and_journal_has_one_owner() {
        let directory = std::env::temp_dir().join(format!(
            "benchmark-journal-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut journal = WriteJournal::open(&directory, &["test-database", "test-region"])
            .await
            .unwrap();
        journal.record(vec![120, 180]).await.unwrap();
        assert!(
            WriteJournal::open(&directory, &["test-database", "test-region"])
                .await
                .is_err()
        );
        drop(journal);
        let mut recovered = WriteJournal::open(&directory, &["test-database", "test-region"])
            .await
            .unwrap();
        assert_eq!(recovered.pending, [120, 180]);
        recovered.record(Vec::new()).await.unwrap();
        drop(recovered);
        let cleared = WriteJournal::open(&directory, &["test-database", "test-region"])
            .await
            .unwrap();
        assert!(cleared.pending.is_empty());
        drop(cleared);
        std::fs::remove_dir_all(directory).unwrap();
    }

    // Run by test:rollups in a disposable database with production table types
    // and a constraint that rejects the final missed row after its intent lands.
    #[tokio::test]
    #[ignore = "requires the disposable ClickHouse fixture created by test:rollups"]
    async fn failed_final_batch_fixture() -> anyhow::Result<()> {
        let database = std::env::var("BENCH_CH_DATABASE")?;
        anyhow::ensure!(database.starts_with("benchmark_rollup_test_"));
        let client = Client::default()
            .with_url(std::env::var("BENCH_CH_URL")?)
            .with_database(&database)
            .with_user(std::env::var("BENCH_CH_USER")?)
            .with_password(std::env::var("BENCH_CH_PASSWORD")?);
        validate_schema(&client).await?;
        let mut journal = WriteJournal::open(
            std::path::Path::new(&std::env::var("BENCH_TEST_STATE_DIR")?),
            &[&database],
        )
        .await?;
        if std::env::var("BENCH_TEST_RECOVER").as_deref() == Ok("1") {
            let subscriber = SubscriberConfig {
                region: "us-east".into(),
                cloud_provider: "example-cloud".into(),
                latitude: 0.,
                longitude: 0.,
            };
            journal.recover(&client, &subscriber).await?;
            return Ok(());
        }
        let first = SystemTime::UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs()
                    / 60
                    * 60
                    + 1,
            );
        let kind = stream(DataType::Slot);
        let (tx, rx) = mpsc::channel(3);
        for (provider, outcome) in [(0, model::Outcome::OnTime), (1, model::Outcome::Missed)] {
            tx.send(StorageRecord::Lag(model::LagRecord {
                message_bytes: None,
                stream: kind,
                key: EventKey::Slot {
                    slot: 100,
                    status: 0,
                },
                first_utc: first,
                provider,
                outcome,
                lag_ns: (provider == 0).then_some(0),
            }))
            .await?;
            if provider == 0 {
                tx.send(StorageRecord::Coverage(model::CoverageRecord {
                    stream: kind,
                    provider: 1,
                    start: first - Duration::from_secs(1),
                    end: first + Duration::from_secs(59),
                }))
                .await?;
            }
        }
        drop(tx);
        let ctx = context();
        let error = write(
            client,
            journal,
            ctx.cloud_provider,
            ctx.region,
            ctx.providers,
            ctx.versions,
            rx,
            BatchLimits {
                max_records: 1,
                flush_interval: Duration::from_secs(3600),
            },
        )
        .await
        .expect_err("the final raw insert must be rejected");
        let expected_error = if std::env::var("BENCH_TEST_FAIL_INTENT").as_deref() == Ok("1") {
            "raw write intent failed"
        } else {
            "benchmark_event_lags insert failed"
        };
        anyhow::ensure!(error.message.contains(expected_error), "{error}");
        anyhow::ensure!(error.unacknowledged_records == 1);
        Ok(())
    }
    fn stream(data_type: DataType) -> StreamKind {
        StreamKind {
            data_type,
            commitment: Commitment::Processed,
        }
    }
    fn context() -> Context {
        Context {
            cloud_provider: "example-cloud".into(),
            region: "us-east".into(),
            providers: vec!["a".into(), "b".into()],
            versions: std::array::from_fn(|index| SubscriptionVersion {
                id: format!("id-{index}"),
                filters: "{}".into(),
            }),
        }
    }
    #[tokio::test]
    async fn schema_fetch_and_insert_time_out_when_http_peer_never_responds() {
        for validation in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let _ = accepted_tx.send(());
                let _socket = socket;
                std::future::pending::<()>().await;
            });
            let client = Client::default()
                .with_url(format!("http://{address}"))
                .with_validation(validation);
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                insert_rows_with_timeouts(
                    &client,
                    "benchmark_subscription_versions",
                    &[VersionRow {
                        subscription_id: "test".into(),
                        data_type: "slot".into(),
                        commitment: "processed".into(),
                        filters: "unfiltered".into(),
                        registered_utc: utc(SystemTime::now()),
                    }],
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                ),
            )
            .await
            .expect("insert must return within finite bound");
            accepted_rx.await.expect("test server accepted insert");
            assert!(matches!(result, Err(clickhouse::error::Error::TimedOut)));
            server.abort();
        }
    }

    #[test]
    fn comparison_and_coverage_queries_scope_identity_to_region() {
        let sql = include_str!("../sql/reports.sql");
        let head_to_head = sql
            .split("-- HEAD-TO-HEAD:")
            .nth(1)
            .unwrap()
            .split("-- OBSERVED LEADERBOARD")
            .next()
            .unwrap();
        assert!(head_to_head.contains("AND region = {region:String}"));
        assert!(head_to_head.contains("GROUP BY region, cloud_provider, subscription_id"));
        assert!(head_to_head.contains("ON a.region = b.region"));
        assert!(head_to_head.contains("a.cloud_provider = b.cloud_provider"));
        let coverage = sql.split("-- SHORT-RANGE COVERAGE GATE:").nth(1).unwrap();
        assert!(coverage.contains("WHERE region = {region:String}"));
        assert!(coverage.contains("AND cloud_provider = {cloud_provider:String}"));
    }

    #[test]
    fn retired_duplicate_slot_stream_is_rejected_by_the_sink() {
        let ctx = context();
        let canonical = StreamKind {
            data_type: DataType::Slot,
            commitment: Commitment::Processed,
        };
        assert_eq!(ctx.subscription(canonical).unwrap(), "id-4");
        let retired = StreamKind {
            commitment: Commitment::Finalized,
            ..canonical
        };
        assert!(ctx
            .subscription(retired)
            .unwrap_err()
            .contains("inactive stream"));
    }

    #[test]
    fn subscriber_location_mapping_preserves_coordinates() {
        let subscriber = SubscriberConfig {
            region: "frankfurt".into(),
            cloud_provider: "example-cloud".into(),
            latitude: 50.1109,
            longitude: 8.6821,
        };
        let row = SubscriberRow::new(&subscriber, SystemTime::UNIX_EPOCH);
        assert_eq!(row.region, "frankfurt");
        assert_eq!(row.cloud_provider, "example-cloud");
        assert_eq!((row.latitude, row.longitude), (50.1109, 8.6821));
        assert_eq!(row.registered_utc.unix_timestamp_nanos(), 0);
    }

    #[test]
    fn stable_event_id_is_stream_scoped_and_account_includes_transaction() {
        let account = EventKey::Account {
            slot: 42,
            pubkey: vec![1, 2],
            txn_signature: vec![3; 64],
        };
        let hash = event_id(stream(DataType::Account), &account);
        assert_eq!(
            hash.to_hex().as_str(),
            "307a73a79589e8ba94bb863a330f4863e1ed1428535bd5fe7e1201443b658d58"
        );
        assert_eq!(hash, event_id(stream(DataType::Account), &account));
        let another_transaction = EventKey::Account {
            slot: 42,
            pubkey: vec![1, 2],
            txn_signature: vec![4; 64],
        };
        assert_ne!(
            hash,
            event_id(stream(DataType::Account), &another_transaction)
        );
        assert_ne!(
            stored_event_id(stream(DataType::Account), &account),
            stored_event_id(stream(DataType::Account), &another_transaction)
        );
        assert_ne!(
            hash,
            event_id(
                StreamKind {
                    commitment: Commitment::Finalized,
                    ..stream(DataType::Account)
                },
                &account
            )
        );
        assert_ne!(
            hash,
            event_id(
                stream(DataType::Account),
                &EventKey::Account {
                    slot: 43,
                    pubkey: vec![1, 2],
                    txn_signature: vec![3; 64],
                }
            )
        );
        assert_ne!(
            hash,
            event_id(
                stream(DataType::Account),
                &EventKey::Account {
                    slot: 42,
                    pubkey: vec![1, 3],
                    txn_signature: vec![3; 64],
                }
            )
        );
        assert_ne!(
            hash,
            event_id(
                stream(DataType::Slot),
                &EventKey::Slot {
                    slot: 42,
                    status: 1
                }
            )
        );
    }
    #[test]
    fn row_mapping_preserves_lag_outcome_and_unknown_coverage() {
        let ctx = context();
        let first_utc = SystemTime::UNIX_EPOCH + Duration::from_nanos(1_234_567_890);
        let stream = stream(DataType::Account);
        let key = EventKey::Account {
            slot: 7,
            pubkey: vec![9; 32],
            txn_signature: vec![8; 64],
        };
        let row = ctx
            .lag(&model::LagRecord {
                message_bytes: Some(1234),
                stream,
                key: key.clone(),
                first_utc,
                provider: 1,
                outcome: model::Outcome::OnTime,
                lag_ns: Some(123),
            })
            .unwrap();
        assert_eq!(
            (
                row.slot,
                row.provider.as_str(),
                row.subscription_id.as_str()
            ),
            (7, "b", "id-0")
        );
        assert_eq!(row.cloud_provider, "example-cloud");
        assert_eq!(row.region, "us-east");
        assert_eq!(row.lag_ns, Some(123));
        assert_eq!(row.message_bytes, Some(1234));
        assert_eq!(row.event_utc.unix_timestamp_nanos(), 1_234_567_890);
        assert_eq!(row.event_id, stored_event_id(stream, &key));
        // The stored ID is exactly the leading 16 bytes of the full identity hash.
        assert_eq!(
            row.event_id.to_le_bytes(),
            event_id(stream, &key).as_bytes()[..16]
        );
        let missed = ctx
            .lag(&model::LagRecord {
                message_bytes: None,
                stream,
                key,
                first_utc,
                provider: 0,
                outcome: model::Outcome::Missed,
                lag_ns: None,
            })
            .unwrap();
        assert_eq!(missed.outcome, "missed");
        assert_eq!(missed.lag_ns, None);
        assert_eq!(missed.message_bytes, None);
        let gap = ctx
            .gap(&model::GapRecord {
                stream,
                provider: None,
                utc: first_utc,
                reason: "queue full".into(),
            })
            .unwrap();
        assert_eq!(gap.cloud_provider, "example-cloud");
        assert_eq!(gap.region, "us-east");
        assert_eq!(gap.status, "unknown");
        assert_eq!(gap.provider, None);
        assert_eq!(gap.interval_end, Some(utc(first_utc)));
        assert!(ctx
            .lag(&model::LagRecord {
                message_bytes: None,
                stream,
                key: EventKey::Slot { slot: 7, status: 0 },
                first_utc,
                provider: 0,
                outcome: model::Outcome::OnTime,
                lag_ns: Some(0)
            })
            .is_err());
    }
}
