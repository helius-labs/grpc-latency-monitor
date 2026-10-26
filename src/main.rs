use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use solana_streaming_benchmark::{
    config::Config,
    forks::SlotFates,
    model::{Commitment, DataType, StreamKind},
    pipeline,
    rollups::{self, Worker},
    storage::{self, BatchLimits},
    stream::{self, InFlight},
    versions,
};
use tokio::sync::{mpsc, watch};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn client(config: &Config) -> Result<clickhouse::Client> {
    Ok(clickhouse::Client::default()
        .with_url(&config.clickhouse.url)
        .with_database(&config.clickhouse.database)
        .with_user(std::env::var(&config.clickhouse.user_env).context("ClickHouse user missing")?)
        .with_password(
            std::env::var(&config.clickhouse.password_env)
                .context("ClickHouse password missing")?,
        ))
}

/// How long matchers get to write their pending rows on shutdown.
const MATCHER_STOP_TIMEOUT: Duration = Duration::from_secs(60);

async fn drain_matchers(
    matchers: &mut tokio::task::JoinSet<Result<()>>,
    timeout: Duration,
) -> Result<()> {
    let result = tokio::time::timeout(timeout, async {
        let mut failure = None;
        while let Some(result) = matchers.join_next().await {
            let result = result
                .context("stream task panicked or was cancelled")
                .and_then(|r| r);
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            matchers.abort_all();
            while matchers.join_next().await.is_some() {}
            bail!("stream tasks did not stop in time; pending rows lost; coverage unknown")
        }
    }
}

/// Worker threads for matching, the ClickHouse writer and summaries. None of them is
/// latency critical: they only consume timestamps that readers already took.
const PIPELINE_THREADS: usize = 2;

fn main() -> Result<()> {
    match dotenvy::from_path(".env") {
        Ok(()) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        // Parse errors may contain credentials, so do not include the underlying error.
        Err(_) => bail!("failed to load .env; check file access and syntax"),
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let config = Config::from_file(args.config)?;
    // Readers only receive, stamp, decode and enqueue. Their own runtime keeps
    // matching, ClickHouse serialization/compression and summary work from ever
    // delaying a timestamp (tonic's connection tasks follow the reader runtime).
    let cores = std::thread::available_parallelism().map_or(2, |cores| cores.get());
    let readers = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores.saturating_sub(PIPELINE_THREADS).max(2))
        .thread_name("stream-reader")
        .enable_all()
        .build()
        .context("starting reader runtime")?;
    let pipeline = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(PIPELINE_THREADS)
        .thread_name("pipeline")
        .enable_all()
        .build()
        .context("starting pipeline runtime")?;
    let result = pipeline.block_on(run(config, readers.handle().clone()));
    readers.shutdown_timeout(Duration::from_secs(5));
    result
}

async fn run(config: Config, readers: tokio::runtime::Handle) -> Result<()> {
    let client = client(&config)?;
    storage::validate_schema(&client).await?;
    let database_key = format!(
        "{}/{}",
        config.clickhouse.url.trim_end_matches('/'),
        config.clickhouse.database
    );
    let rollups = Worker::new(
        client.clone(),
        &database_key,
        &config.subscriber.region,
        &config.subscriber.cloud_provider,
        rollups::settle_delay(
            config.window,
            config.processing_grace,
            config.flush_interval,
        ),
    )
    .await?;
    let mut journal = storage::WriteJournal::open(
        &config.state_dir,
        &[
            &database_key,
            &config.subscriber.region,
            &config.subscriber.cloud_provider,
        ],
    )
    .await?;
    journal.recover(&client, &config.subscriber).await?;
    storage::register_subscriber(&client, &config.subscriber).await?;
    let (rollup_shutdown, rollup_stop) = tokio::sync::oneshot::channel();
    let mut rollup_task = tokio::spawn(rollups.run(rollup_stop));
    let mut rollup_task_done = false;
    tracing::info!(
        cloud_provider = %config.subscriber.cloud_provider,
        region = %config.subscriber.region,
        providers = config.providers.len(),
        streams_per_provider = 6,
        "starting benchmark"
    );
    let (storage_tx, storage_rx) = mpsc::channel(config.storage_buffer_capacity);
    let mut writer = tokio::spawn(storage::write(
        client,
        journal,
        config.subscriber.cloud_provider.clone(),
        config.subscriber.region.clone(),
        config.providers.iter().map(|p| p.name.clone()).collect(),
        versions::subscription_versions(&config.filters, config.window, &config.providers),
        storage_rx,
        BatchLimits {
            max_records: config.batch_size,
            flush_interval: config.flush_interval,
        },
    ));
    let mut writer_done = false;
    let mut matchers = tokio::task::JoinSet::new();
    let mut stream_readers = tokio::task::JoinSet::new();
    let (stop_matchers, stop) = watch::channel(false);
    let fates = Arc::new(SlotFates::default());
    for data_type in DataType::ALL {
        for commitment in Commitment::ALL {
            let kind = StreamKind {
                data_type,
                commitment,
            };
            let (arrivals_tx, arrivals_rx) = mpsc::channel(config.arrival_buffer_capacity);
            let (controls_tx, controls_rx) = mpsc::channel(config.providers.len() * 4);
            let in_flight = Arc::new(InFlight::new(config.providers.len()));
            matchers.spawn(pipeline::match_stream(
                kind,
                config.providers.len(),
                config.clone(),
                arrivals_rx,
                controls_rx,
                in_flight.clone(),
                fates.clone(),
                stop.clone(),
                storage_tx.clone(),
                pipeline::COVERAGE_INTERVAL,
            ));
            for (i, provider) in config.providers.iter().enumerate() {
                let provider = provider.clone();
                let filters = config.filters.clone();
                let arrivals = arrivals_tx.clone();
                let controls = controls_tx.clone();
                let in_flight = in_flight.clone();
                stream_readers.spawn_on(
                    stream::read_stream(i, provider, kind, filters, arrivals, controls, in_flight),
                    &readers,
                );
            }
        }
    }
    drop(storage_tx);
    let mut failure = tokio::select! {
        result = shutdown() => {
            tracing::info!("shutdown requested");
            result.err()
        }
        result = &mut rollup_task => {
            rollup_task_done = true;
            Some(anyhow!("rollup task stopped unexpectedly: {result:?}"))
        }
        result = &mut writer => {
            writer_done = true;
            Some(anyhow!("ClickHouse writer stopped: {result:?}; coverage unknown"))
        }
        result = matchers.join_next() => {
            Some(anyhow!("stream task stopped: {result:?}; coverage unknown"))
        }
        result = stream_readers.join_next() => {
            Some(anyhow!("stream reader stopped: {result:?}; coverage unknown"))
        }
    };
    if let Some(error) = &failure {
        tracing::error!("{error:#}; stopping");
    }
    // Stop matchers before readers: closing their input would end them with an
    // error first. Each writes certified time still pending and undecided misses,
    // then exits; no further interval is certified. Dropping their storage senders
    // lets the writer flush every accepted record, on failure too.
    let _ = stop_matchers.send(true);
    if let Err(error) = drain_matchers(&mut matchers, MATCHER_STOP_TIMEOUT).await {
        tracing::error!("{error:#}");
        failure.get_or_insert(error);
    }
    stream_readers.abort_all();
    while stream_readers.join_next().await.is_some() {}
    let written = if writer_done {
        Ok(())
    } else {
        writer
            .await
            .context("writer task panicked")?
            .map(drop)
            .map_err(anyhow::Error::from)
    };
    let summarized = if rollup_task_done {
        Ok(())
    } else {
        let _ = rollup_shutdown.send(failure.is_none() && written.is_ok());
        rollup_task.await.context("rollup task panicked")?
    };
    match failure {
        Some(error) => Err(error),
        None => written.and(summarized),
    }
}

async fn shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_a_slow_storage_drain() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(()).await.unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { tx.send(()).await.context("writer stopped") });
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            rx.recv().await;
            rx.recv().await;
        });
        drain_matchers(&mut tasks, MATCHER_STOP_TIMEOUT)
            .await
            .unwrap();
        writer.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_reports_lost_pending_rows() {
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(std::future::pending::<Result<()>>());
        let error = drain_matchers(&mut tasks, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("pending rows lost"));
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn shutdown_propagates_matcher_failure() {
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async { bail!("failed to persist coverage") });
        assert!(drain_matchers(&mut tasks, MATCHER_STOP_TIMEOUT)
            .await
            .is_err());
    }
}
