//! Subscriber-owned minute summaries. All work is scoped to region and cloud.
use anyhow::{Context, Result};
use clickhouse::{Client, Row};
use serde::Deserialize;
use std::{
    fs::{File, OpenOptions},
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;
use tracing::{error, info};

/// Builds one minute of summaries from raw rows. The shared template in
/// `sql/rollups/build.sql` is also the dashboards' raw fallback, so both always
/// count events the same way.
///
/// Keep the bounds DateTime64(9): mixed DateTime bounds can mis-prune nanosecond
/// MergeTree keys on ClickHouse 26.3. Parameters are ClickHouse typed query
/// parameters. A build recomputes a complete minute; its generation is published
/// separately, only after the build succeeds.
static BUILD: LazyLock<String> = LazyLock::new(|| {
    let template = include_str!("../sql/rollups/build.sql")
        .replace(
            "{{source_filter}}",
            "region = {region:String} AND cloud_provider = {cloud_provider:String}\n \
             AND event_utc >= {bucket:DateTime64(9, 'UTC')} \
             AND event_utc < {bucket:DateTime64(9, 'UTC')} + INTERVAL 1 MINUTE",
        )
        .replace("{{bucket}}", "{bucket:DateTime64(9, 'UTC')}")
        .replace("{{generation}}", "{generation:UInt64}");
    format!(
        "INSERT INTO benchmark_minute_rollups\n{}\n\
         SETTINGS max_threads = 8, max_memory_usage = 4000000000, max_execution_time = 120",
        template.trim_end()
    )
});
const PENDING: &str = include_str!("../sql/rollups/pending.sql");
/// Superseded and orphaned generations are never selected (dashboards read only the
/// latest committed generation per minute), but they would otherwise be stored for 30
/// days. Delete a generation only when it is older than `cutoff` and its minute's
/// latest commit is also older than `cutoff`: a dashboard query that captured the
/// previous manifest moments before a newer commit can still read its generation.
/// Generations are build-start timestamps (ns), and builds time out after 150 s, so a
/// generation older than `cutoff` is never still being built.
const CLEANUP: &str = include_str!("../sql/rollups/cleanup.sql");
/// How long a superseded generation stays readable, and how often cleanup runs.
/// Much longer than any dashboard query (45-60 s), and lightweight deletes are
/// mutations, so they are batched rather than run per minute.
const CLEANUP_AGE: Duration = Duration::from_secs(600);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(600);
const COMMIT: &str = "INSERT INTO benchmark_rollup_commits (region, cloud_provider, bucket, generation, revision) VALUES \
    ({region:String}, {cloud_provider:String}, {bucket:DateTime64(9, 'UTC')}, {generation:UInt64}, {revision:UInt64})";

/// How long after a minute ends before its rows are complete. A window closes up
/// to `window + processing_grace` plus one matcher tick (1s) after its first
/// arrival, then rows wait up to `flush_interval` before insertion; the 5s margin
/// absorbs insert latency and clock skew between the subscriber and ClickHouse.
/// Building earlier stays correct (rows inserted afterwards mark the minute dirty) but
/// rebuilds it: with a 5s window, building ~6s after the minute rebuilt 68% of
/// minutes, and every superseded generation is retained for 30 days.
pub fn settle_delay(
    window: Duration,
    processing_grace: Duration,
    flush_interval: Duration,
) -> Duration {
    window + processing_grace + flush_interval + Duration::from_secs(1) + Duration::from_secs(5)
}

pub struct Worker {
    client: Client,
    region: String,
    cloud_provider: String,
    settle: Duration,
    _lock: File,
}
#[derive(Debug, Row, Deserialize)]
struct Pending {
    bucket: String,
    revision: u64,
    generation: u64,
}

fn generation(previous: u64, now: SystemTime) -> Result<u64> {
    let nanos = u64::try_from(now.duration_since(UNIX_EPOCH)?.as_nanos())?;
    Ok(nanos.max(previous.checked_add(1).context("generation overflow")?))
}

impl Worker {
    /// Exactly one subscriber may own a region/cloud combination. The local lock
    /// catches duplicate processes on this host; deployment must ensure uniqueness
    /// across hosts too. The writer uses a separate client without rollup limits.
    pub async fn new(
        client: Client,
        database_key: &str,
        region: &str,
        cloud_provider: &str,
        settle: Duration,
    ) -> Result<Self> {
        let mut hash = blake3::Hasher::new();
        for field in [database_key, region, cloud_provider] {
            hash.update(&(field.len() as u64).to_le_bytes());
            hash.update(field.as_bytes());
        }
        let key = hash.finalize();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(std::env::temp_dir().join(format!("benchmark-rollups-{}.lock", key.to_hex())))?;
        lock.try_lock()
            .context("another subscriber owns this region/cloud on this host")?;
        let worker = Self {
            client: client
                .with_setting("max_threads", "8")
                .with_setting("max_memory_usage", "4000000000")
                .with_setting("max_execution_time", "120"),
            region: region.into(),
            cloud_provider: cloud_provider.into(),
            settle,
            _lock: lock,
        };
        // Fail before subscriptions start if the scoped rollup migration is missing.
        tokio::time::timeout(
            Duration::from_secs(30),
            worker
                .client
                .query(PENDING)
                .param("region", region)
                .param("cloud_provider", cloud_provider)
                .param("include_open", 0_u8)
                .param("settle_ms", 0_u64)
                .param("limit", 0_u64)
                .fetch_all::<Pending>(),
        )
        .await
        .context("rollup schema check timed out")?
        .context("rollup schema check failed; apply all SQL migrations")?;
        Ok(worker)
    }

    /// Errors retry independently of the measurement path. Shutdown is signaled
    /// only after the storage writer flushes, then includes the final open minute.
    pub async fn run(self, mut shutdown: oneshot::Receiver<()>) -> Result<()> {
        info!(
            region = self.region,
            cloud_provider = self.cloud_provider,
            "Subscriber rollups started"
        );
        let mut last_cleanup: Option<tokio::time::Instant> = None;
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                result = self.drain(false) => {
                    if let Err(error) = result {
                        error!(error = %format!("{error:#}"), "Rollup failed; will retry");
                    }
                }
            }
            if last_cleanup.is_none_or(|at| at.elapsed() >= CLEANUP_INTERVAL) {
                last_cleanup = Some(tokio::time::Instant::now());
                tokio::select! {
                    _ = &mut shutdown => break,
                    result = self.cleanup(SystemTime::now()) => {
                        if let Err(error) = result {
                            error!(error = %format!("{error:#}"), "Summary cleanup failed; will retry");
                        }
                    }
                }
            }
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
        }
        tokio::time::timeout(Duration::from_secs(150), self.drain(true))
            .await
            .context("final rollup flush timed out; restart within five hours to retry")??;
        info!("Subscriber rollups flushed");
        Ok(())
    }
    async fn publish(&self, minute: &Pending) -> Result<()> {
        // Every attempt gets a new generation. An ambiguous build may leave orphan
        // rows, but they are never selected without an acknowledged build + manifest.
        let generation = generation(minute.generation, SystemTime::now())?;
        tokio::time::timeout(Duration::from_secs(150), async {
            self.client
                .query(&BUILD)
                .param("region", &self.region)
                .param("cloud_provider", &self.cloud_provider)
                .param("bucket", &minute.bucket)
                .param("generation", generation)
                .execute()
                .await
                .context("build complete minute")?;
            self.client
                .query(COMMIT)
                .param("region", &self.region)
                .param("cloud_provider", &self.cloud_provider)
                .param("bucket", &minute.bucket)
                .param("generation", generation)
                .param("revision", minute.revision)
                .execute()
                .await
                .context("publish completed generation")?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("minute build/publication timed out")??;
        info!(bucket = minute.bucket, generation, "Published minute");
        Ok(())
    }

    /// Delete superseded and orphaned generations (see [`CLEANUP`]).
    async fn cleanup(&self, now: SystemTime) -> Result<()> {
        let cutoff = u64::try_from(
            now.checked_sub(CLEANUP_AGE)
                .unwrap_or(UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)?
                .as_nanos(),
        )?;
        tokio::time::timeout(
            Duration::from_secs(120),
            self.client
                .query(CLEANUP)
                .param("region", &self.region)
                .param("cloud_provider", &self.cloud_provider)
                .param("cutoff", cutoff)
                .execute(),
        )
        .await
        .context("summary cleanup timed out")?
        .context("delete superseded summary generations")?;
        Ok(())
    }

    /// At shutdown (`include_open`) the writer has already flushed every row, so
    /// all minutes including the open one are complete: no settle delay applies.
    async fn drain(&self, include_open: bool) -> Result<()> {
        let settle_ms = if include_open {
            0
        } else {
            u64::try_from(self.settle.as_millis()).unwrap_or(u64::MAX)
        };
        loop {
            let work = tokio::time::timeout(
                Duration::from_secs(30),
                self.client
                    .query(PENDING)
                    .param("region", &self.region)
                    .param("cloud_provider", &self.cloud_provider)
                    .param("include_open", u8::from(include_open))
                    .param("settle_ms", settle_ms)
                    .param("limit", 60_u64)
                    .fetch_all::<Pending>(),
            )
            .await
            .context("dirty-minute lookup timed out")??;
            if work.is_empty() {
                return Ok(());
            }
            for minute in work {
                self.publish(&minute).await?;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // Driven by grafana/tests/rollups.py against its disposable database. This
    // exercises the subscriber's real worker without a production maintenance CLI.
    #[tokio::test]
    #[ignore = "requires the disposable ClickHouse fixture created by test:rollups"]
    async fn publish_fixture() -> Result<()> {
        let database = std::env::var("BENCH_CH_DATABASE")?;
        anyhow::ensure!(
            database.starts_with("benchmark_rollup_test_"),
            "fixture database required"
        );
        let region = std::env::var("ROLLUP_TEST_REGION")?;
        let cloud = std::env::var("ROLLUP_TEST_CLOUD")?;
        let url = std::env::var("BENCH_CH_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
        let key = format!("{}/{database}", url.trim_end_matches('/'));
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(std::env::var("BENCH_CH_USER")?)
            .with_password(std::env::var("BENCH_CH_PASSWORD")?);
        let worker = Worker::new(client, &key, &region, &cloud, Duration::ZERO).await?;
        if std::env::var("ROLLUP_TEST_SHUTDOWN").as_deref() == Ok("1") {
            let (send, receive) = oneshot::channel();
            send.send(()).unwrap();
            worker.run(receive).await
        } else {
            worker.drain(true).await
        }
    }

    #[test]
    fn build_replaces_every_template_placeholder() {
        assert!(!BUILD.contains("{{"), "{}", *BUILD);
        assert!(BUILD.starts_with("INSERT INTO benchmark_minute_rollups\n"));
    }

    #[test]
    fn settle_delay_covers_window_close_and_flush() {
        let settle = settle_delay(
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(1),
        );
        // 5s window + 2s grace + 1s tick + 1s flush + 5s margin.
        assert_eq!(settle, Duration::from_secs(14));
    }

    #[test]
    fn generation_advances_even_after_clock_rollback() {
        let now = UNIX_EPOCH + Duration::from_nanos(100);
        assert_eq!(generation(90, now).unwrap(), 100);
        assert_eq!(generation(100, now).unwrap(), 101);
        assert_eq!(generation(200, now).unwrap(), 201);
        assert!(generation(u64::MAX, now).is_err());
    }
}
