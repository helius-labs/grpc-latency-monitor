//! Independent RPC slot references. No RPC work runs on the reader runtime.
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tokio::{sync::watch, time::Instant};
use yellowstone_grpc_proto::geyser::SlotStatus;

use crate::{
    config::SlotValidationConfig,
    model::{Commitment, EventKey, StreamKind},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tips {
    pub processed: u64,
    pub confirmed: u64,
    pub finalized: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Reference {
    pub tips: Option<Tips>,
    /// Request start, so network latency consumes freshness rather than extending it.
    observed_at: Instant,
    pub healthy_since: (Instant, SystemTime),
    /// Detect outages even if a busy matcher misses both expiry and recovery.
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Unavailable,
    Stale,
    Future,
}

impl Reference {
    pub fn empty() -> Self {
        let now = Instant::now();
        Self {
            tips: None,
            observed_at: now,
            healthy_since: (now, SystemTime::now()),
            generation: 0,
        }
    }

    pub fn fresh(&self, now: Instant, max_age: Duration) -> bool {
        self.tips.is_some() && now.saturating_duration_since(self.observed_at) < max_age
    }

    /// Lagging load-balanced backends are merged into the highest tips seen, and
    /// refused only beyond `tolerance`. Freshness renews only when the processed
    /// tip advances, so a stuck backend expires instead of rejecting live slots.
    pub(crate) fn advance(
        &self,
        tips: Tips,
        requested: Instant,
        received: Instant,
        utc: SystemTime,
        max_age: Duration,
        tolerance: u64,
    ) -> Option<Self> {
        let merged = match self.tips {
            None => tips,
            Some(old) => {
                if [
                    (tips.processed, old.processed),
                    (tips.confirmed, old.confirmed),
                    (tips.finalized, old.finalized),
                ]
                .iter()
                .any(|(new, old)| *new < old.saturating_sub(tolerance))
                {
                    return None;
                }
                Tips {
                    processed: tips.processed.max(old.processed),
                    confirmed: tips.confirmed.max(old.confirmed),
                    finalized: tips.finalized.max(old.finalized),
                }
            }
        };
        if self
            .tips
            .is_some_and(|old| merged.processed <= old.processed)
        {
            return Some(Self {
                tips: Some(merged),
                ..*self
            });
        }
        let recovering = !self.fresh(received, max_age);
        Some(Self {
            tips: Some(merged),
            observed_at: requested,
            healthy_since: if recovering {
                (received, utc)
            } else {
                self.healthy_since
            },
            generation: self.generation + u64::from(recovering),
        })
    }

    /// Whether this reference was renewed by a poll started at `requested`.
    pub(crate) fn renewed_by(&self, requested: Instant) -> bool {
        self.observed_at == requested
    }

    pub fn check(
        &self,
        kind: StreamKind,
        key: &EventKey,
        at: Instant,
        now: Instant,
        config: &SlotValidationConfig,
    ) -> Result<(), Rejection> {
        if !self.fresh(now, config.max_reference_age) || at < self.healthy_since.0 {
            return Err(Rejection::Unavailable);
        }
        let tips = self.tips.ok_or(Rejection::Unavailable)?;
        let tip = match key {
            EventKey::Slot { status, .. } => match SlotStatus::try_from(*status) {
                Ok(SlotStatus::SlotFinalized) => tips.finalized,
                Ok(SlotStatus::SlotConfirmed) => tips.confirmed,
                _ => tips.processed,
            },
            _ => match kind.commitment {
                Commitment::Processed => tips.processed,
                Commitment::Finalized => tips.finalized,
            },
        };
        let slot = key.slot();
        if slot < tip.saturating_sub(config.slot_buffer) {
            return Err(Rejection::Stale);
        }
        if slot > tip.saturating_add(config.slot_buffer) {
            return Err(Rejection::Future);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fixture(tips: Tips) -> Self {
        let mut reference = Self::empty();
        reference.tips = Some(tips);
        reference.healthy_since.0 -= Duration::from_secs(3600);
        reference.healthy_since.1 -= Duration::from_secs(3600);
        reference
    }
}

/// Only fields needed from JSON-RPC are retained. Server messages may include secrets.
#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    result: Option<u64>,
    error: Option<serde_json::Value>,
}

async fn fetch_slot(client: &reqwest::Client, url: &str, commitment: &str, id: u64) -> Result<u64> {
    let mut response = client
        .post(url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "getSlot", "params": [{"commitment": commitment}]
        }))
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("reference RPC transport failed"))?;
    if !response.status().is_success() {
        bail!("reference RPC returned an unsuccessful HTTP status");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow::anyhow!("reference RPC response failed"))?
    {
        if body.len() + chunk.len() > 16_384 {
            bail!("reference RPC response exceeded size limit");
        }
        body.extend_from_slice(&chunk);
    }
    let response: RpcResponse = serde_json::from_slice(&body)
        .map_err(|_| anyhow::anyhow!("invalid reference RPC response"))?;
    if response.jsonrpc != "2.0" || response.id != id || response.error.is_some() {
        bail!("reference RPC returned an invalid or failed JSON-RPC response");
    }
    response
        .result
        .context("reference RPC response has no slot")
}

async fn fetch(client: &reqwest::Client, url: &str) -> Result<Tips> {
    let (processed, confirmed, finalized) = tokio::try_join!(
        fetch_slot(client, url, "processed", 1),
        fetch_slot(client, url, "confirmed", 2),
        fetch_slot(client, url, "finalized", 3),
    )?;
    Ok(Tips {
        processed,
        confirmed,
        finalized,
    })
}

/// Failed polls leave the previous reference usable only until its fixed expiry.
/// The owner monitors this task; unexpected termination stops the benchmark.
pub async fn run(
    config: SlotValidationConfig,
    url: String,
    updates: watch::Sender<Reference>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| anyhow::anyhow!("building reference RPC client failed"))?;
    let mut poll = tokio::time::interval(config.poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut failed = false;
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            _ = poll.tick() => {}
        }
        let requested = Instant::now();
        let result = tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            result = fetch(&client, &url) => result,
        };
        let next = result.ok().and_then(|tips| {
            updates.borrow().advance(
                tips,
                requested,
                Instant::now(),
                SystemTime::now(),
                config.max_reference_age,
                config.slot_buffer,
            )
        });
        let renewed = next.is_some_and(|next| next.renewed_by(requested));
        if let Some(next) = next {
            updates.send_replace(next);
        }
        if renewed {
            if failed {
                tracing::info!("reference RPC polling recovered");
            }
            failed = false;
        } else {
            if !failed {
                tracing::warn!("reference RPC poll failed, regressed or did not advance; coverage will become unknown when the reference expires");
            }
            failed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, model::DataType};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn tips() -> Tips {
        Tips {
            processed: 1040,
            confirmed: 1038,
            finalized: 1000,
        }
    }
    fn kind(commitment: Commitment) -> StreamKind {
        StreamKind {
            data_type: DataType::Transaction,
            commitment,
        }
    }
    fn tx(slot: u64) -> EventKey {
        EventKey::Transaction {
            slot,
            signature: vec![1],
        }
    }

    #[test]
    fn bounds_are_inclusive_and_use_actual_commitment_or_slot_status() {
        let config = Config::example().slot_validation;
        let reference = Reference::fixture(tips());
        let now = Instant::now();
        for (commitment, tip) in [(Commitment::Processed, 1040), (Commitment::Finalized, 1000)] {
            for slot in [tip - 25, tip + 25] {
                assert_eq!(
                    reference.check(kind(commitment), &tx(slot), now, now, &config),
                    Ok(())
                );
            }
            assert_eq!(
                reference.check(kind(commitment), &tx(tip - 26), now, now, &config),
                Err(Rejection::Stale)
            );
            assert_eq!(
                reference.check(kind(commitment), &tx(tip + 26), now, now, &config),
                Err(Rejection::Future)
            );
        }
        // Slot status chooses the reference independently of the stream label.
        for commitment in Commitment::ALL {
            for (status, slot) in [
                (SlotStatus::SlotProcessed, 1040),
                (SlotStatus::SlotConfirmed, 1038),
                (SlotStatus::SlotFinalized, 1000),
            ] {
                let key = EventKey::Slot {
                    slot,
                    status: status as i32,
                };
                assert_eq!(
                    reference.check(kind(commitment), &key, now, now, &config),
                    Ok(())
                );
            }
            let key = EventKey::Slot {
                slot: 1040,
                status: SlotStatus::SlotFinalized as i32,
            };
            assert_eq!(
                reference.check(kind(commitment), &key, now, now, &config),
                Err(Rejection::Future)
            );
        }
    }

    #[test]
    fn bounds_saturate_at_integer_limits() {
        let config = Config::example().slot_validation;
        let now = Instant::now();
        for tip in [0, u64::MAX] {
            let reference = Reference::fixture(Tips {
                processed: tip,
                confirmed: tip,
                finalized: tip,
            });
            assert_eq!(
                reference.check(kind(Commitment::Processed), &tx(tip), now, now, &config),
                Ok(())
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_expiry_regression_and_recovery_preserve_the_gap() {
        let config = Config::example().slot_validation;
        let (age, buffer) = (config.max_reference_age, config.slot_buffer);
        let t = Instant::now();
        let empty = Reference::empty();
        assert_eq!(
            empty.check(kind(Commitment::Processed), &tx(1040), t, t, &config),
            Err(Rejection::Unavailable)
        );
        let first = empty
            .advance(tips(), t, t, SystemTime::now(), age, buffer)
            .unwrap();
        // A backend beyond the tolerance is refused outright.
        let mut regressed = tips();
        regressed.finalized -= buffer + 1;
        assert!(first
            .advance(regressed, t, t, SystemTime::now(), age, buffer)
            .is_none());
        tokio::time::advance(age).await;
        let now = Instant::now();
        assert_eq!(
            first.check(kind(Commitment::Processed), &tx(1040), now, now, &config),
            Err(Rejection::Unavailable)
        );
        // A tip that does not advance cannot recover the reference.
        let stuck = first
            .advance(tips(), now, now, SystemTime::now(), age, buffer)
            .unwrap();
        assert!(!stuck.renewed_by(now));
        assert!(!stuck.fresh(now, age));
        let mut advanced = tips();
        advanced.processed += 1;
        let next = stuck
            .advance(advanced, now, now, SystemTime::now(), age, buffer)
            .unwrap();
        assert!(next.renewed_by(now));
        assert_eq!(next.generation, first.generation + 1);
        assert_eq!(
            next.check(kind(Commitment::Processed), &tx(1040), t, now, &config),
            Err(Rejection::Unavailable)
        );
        assert_eq!(
            next.check(kind(Commitment::Processed), &tx(1040), now, now, &config),
            Ok(())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lagging_backend_merges_but_a_stuck_one_expires() {
        let config = Config::example().slot_validation;
        let (age, buffer) = (config.max_reference_age, config.slot_buffer);
        let t = Instant::now();
        let first = Reference::empty()
            .advance(tips(), t, t, SystemTime::now(), age, buffer)
            .unwrap();
        // One backend a slot behind on one commitment, ahead on another.
        let mut mixed = tips();
        mixed.processed += 2;
        mixed.confirmed -= 1;
        tokio::time::advance(Duration::from_secs(1)).await;
        let now = Instant::now();
        let merged = first
            .advance(mixed, now, now, SystemTime::now(), age, buffer)
            .unwrap();
        assert!(merged.renewed_by(now));
        assert_eq!(
            merged.tips,
            Some(Tips {
                processed: 1042,
                confirmed: 1038,
                finalized: 1000,
            })
        );
        // Answers that stop advancing keep the merged tips but let them expire.
        let mut reference = merged;
        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(1)).await;
            let now = Instant::now();
            reference = reference
                .advance(tips(), now, now, SystemTime::now(), age, buffer)
                .unwrap();
        }
        assert_eq!(reference.tips, merged.tips);
        assert!(!reference.fresh(Instant::now(), age));
    }

    async fn mock_rpc(
        reply: impl Fn(serde_json::Value) -> String + Send + Sync + 'static,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/private-test-token",
            listener.local_addr().unwrap()
        );
        let reply = std::sync::Arc::new(reply);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let reply = reply.clone();
                connections.spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0; 1024];
                    let (end, length) = loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        if n == 0 { return; }
                        request.extend_from_slice(&buf[..n]);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                            let length = headers.lines().find_map(|l| l.strip_prefix("content-length:")).unwrap().trim().parse::<usize>().unwrap();
                            if request.len() >= end + 4 + length { break (end + 4, length); }
                        }
                    };
                    let request = serde_json::from_slice(&request[end..end + length]).unwrap();
                    let body = reply(request);
                    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
                while connections.try_join_next().is_some() {}
            }
        });
        (url, task)
    }

    #[tokio::test]
    async fn polls_each_commitment_and_recovers_after_failed_polls() {
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state = failed.clone();
        // A live chain: each processed answer is one slot ahead of the last.
        let processed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1040));
        let (url, server) = mock_rpc(move |request| {
            if state.load(std::sync::atomic::Ordering::Relaxed) {
                return serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "error":{"message":"private-test-token"}}).to_string();
            }
            assert_eq!(request["method"], "getSlot");
            let slot = match request["params"][0]["commitment"].as_str().unwrap() {
                "processed" => processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed), "confirmed" => 1038, "finalized" => 1000, _ => panic!("unexpected commitment")
            };
            serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":slot}).to_string()
        }).await;
        let mut config = Config::example().slot_validation;
        config.poll_interval = Duration::from_millis(10);
        config.request_timeout = Duration::from_millis(50);
        config.max_reference_age = Duration::from_millis(100);
        let (updates, mut reference) = watch::channel(Reference::empty());
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(run(config.clone(), url.clone(), updates, shutdown));
        tokio::time::timeout(Duration::from_secs(2), reference.changed())
            .await
            .unwrap()
            .unwrap();
        let first = *reference.borrow_and_update();
        let tips = first.tips.unwrap();
        assert!(tips.processed >= 1040);
        assert_eq!((tips.confirmed, tips.finalized), (1038, 1000));
        failed.store(true, std::sync::atomic::Ordering::Relaxed);
        let error = fetch(&reqwest::Client::new(), &url).await.unwrap_err();
        assert!(!format!("{error:#}").contains("private-test-token"));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!reference
            .borrow()
            .fresh(Instant::now(), config.max_reference_age));
        failed.store(false, std::sync::atomic::Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                reference.changed().await.unwrap();
                if reference.borrow_and_update().generation > first.generation {
                    break;
                }
            }
        })
        .await
        .unwrap();
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn malformed_oversized_and_mismatched_responses_are_rejected_without_payloads() {
        for body in [
            "private-test-token".into(),
            "x".repeat(16_385),
            r#"{"jsonrpc":"2.0","id":99,"result":1000}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":-1}"#.into(),
        ] {
            let (url, server) = mock_rpc(move |_| body.clone()).await;
            let error = fetch_slot(&reqwest::Client::new(), &url, "processed", 1)
                .await
                .unwrap_err();
            let diagnostic = format!("{error:#}");
            assert!(!diagnostic.contains("private-test-token"));
            assert!(!diagnostic.contains(&url));
            server.abort();
        }
    }

    #[tokio::test]
    async fn a_hung_rpc_is_bounded_by_the_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/private-test-token",
            listener.local_addr().unwrap()
        );
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(20))
            .build()
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            fetch_slot(&client, &url, "processed", 1),
        )
        .await
        .unwrap();
        assert_eq!(
            result.unwrap_err().to_string(),
            "reference RPC transport failed"
        );
    }
}
