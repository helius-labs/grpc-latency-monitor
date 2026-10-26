# Methodology

What the benchmark measures, how events are matched across providers, and what its results can and cannot claim. For the benchmark's own measurement noise, see [Accuracy](accuracy.md).

## What it measures

For each provider, the subscriber opens six independent streams, with identical requests across providers:

- account updates, transactions and slot updates,
- each at **processed** and **finalized** commitment.

Account streams can filter by `owner`; transaction streams can filter by `account_include`.

On each subscriber, the same event is matched across providers. Each provider's delivery is recorded as a nanosecond lag behind the first observed arrival (the "ideal" provider). Matching uses a single configurable window, 5 seconds in the example configuration.

Out of scope: TCP network probes, validator probes, slot-leader data and WebSocket comparisons.

## Drops

**An event not delivered within the window counts as dropped.**

- The window opens at the first provider's delivery. A connected provider that has not delivered by the deadline is recorded as `missed`, even if its delivery arrives later. There is no separate "late" outcome.
- Only on-time deliveries enter lag percentiles, wins and head-to-head differences.
- Drops are only counted for a continuously connected, healthy stream. Incomplete intervals are unknown, not dropped.
- After each successful subscription, a provider is eligible for drops (and healthy coverage begins) only after `connection_warmup` (default 1 second; `0s` disables it). A server may apply the filter after accepting the subscription, so events from just before it may be delivered by a slower provider that could never have sent them. Deliveries during the warmup still count, and downtime still uses the subscription time.
- A processed account update or transaction on a fork that never confirms is delivered only by providers that replayed that fork. A processed miss therefore waits for its slot's fate, as reported by any provider on the slot streams: in a confirmed slot it is a drop (`missed`); in a dead slot, a slot passed by a later finalized slot, or a slot still undecided after `slot_confirmation_timeout` (default 30 seconds), it is recorded as `forked`. Forked misses are kept for auditing but are neither drops nor competitors for wins.

## Late deliveries

After a window closes, its key is remembered for `expired_key_retention` (default 2 minutes). A delivery after the window is ignored, so it cannot start a new comparison in which the late provider appears first and the punctual providers are marked dropped.

Up to `expired_key_capacity` exact keys are kept per stream. The default is `matcher_key_capacity` × ⌈`expired_key_retention` / (`window` + `processing_grace` + 1 s)⌉, enough for the whole retention at the rate `matcher_key_capacity` is sized for. If older keys must be forgotten before their retention ends, the highest such slot becomes a floor and any new key at or below it is ignored too. Slot statuses keep a floor per status, since finalized trails processed.

## Configurations

The window and the provider set are part of each configuration's ID. Lag behind ideal and win shares are relative to every configured provider, so changing the window, or adding, removing, renaming or re-pointing a provider (a different endpoint URL), starts a new configuration. Results measured under different definitions are never mixed.

Provider order and tokens are not part of the ID, so a rotated credential continues the same configuration. Stored descriptions and dashboard labels list provider names but show endpoints only as a short hash, since URLs can embed credentials.

## Limits

This is not an independent proof that every chain event was delivered.

- Accounts are matched by `(slot, pubkey)` using only each provider's first update in that slot. Later writes in the same slot are received but **not** individually compared.
- UTC is used for report time ranges; monotonic timestamps are used for differences. Timestamps from different subscribers are never subtracted.

## Message size

Account and transaction arrivals store `message_bytes`: the encoded size of that provider's full protobuf `SubscribeUpdate`, including metadata and the update envelope, excluding gRPC framing, HTTP/2, TLS and compression. Duplicate account writes and deliveries do not add bytes. Missed events and slot updates have NULL sizes, and throughput charts leave gaps for buckets with unknown sizes.

## Coverage

A failed insert, full queue, restart or missing health evidence makes coverage **unknown**, not zero missed messages.

- Inserts use 10-second send and 30-second response timeouts, and the subscriber terminates on failure. The writer uses bounded memory, not a durable spool, and ClickHouse MergeTree does not enforce uniqueness after ambiguous retries.
- On shutdown, or when a task fails, matching stops first: pending certified coverage and undecided processed misses (as `forked`) are written, then every accepted row is flushed before exit. Windows still open are not written; no coverage interval contains them.
- Each one-second tick processes everything already queued before closing windows, so a busy queue does not discard measurements. A full storage queue delays rows and withholds health certification but does not discard windows.
- A window is discarded only when its input may be incomplete: a reader stamped an update more than `processing_grace` ago without enqueuing it, an arrival was processed more than `processing_grace` after its stamp (at most one invalidation per tick), a provider gap, or a capacity overflow affecting that key. Such keys emit no lag classifications, even for providers already observed, and still expire on schedule. After a backlog, new healthy intervals start only once queues drain; the time in between stays unknown.

The raw example leaderboard and miss fraction are **not certified reliability claims**. For a reliability claim, use only event windows fully inside affirmative `healthy` `benchmark_measurement_health` intervals with no intersecting `unknown` gap. Missing coverage, including after crashes or outages, is unknown, not healthy.

Healthy coverage is written at most once a minute per provider and stream; pending certified time is written as soon as health is lost, so batching never drops it. It is certified only after draining controls and arrivals and after preceding ClickHouse inserts are acknowledged; neither a connected event nor the absence of gaps alone proves health.
