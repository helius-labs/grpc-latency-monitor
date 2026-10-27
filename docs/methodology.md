# Methodology

What the benchmark measures, how events are matched across providers, and what its results can and cannot claim. For the benchmark's own measurement noise, see [Accuracy](accuracy.md).

## What it measures

For each provider, the subscriber opens five independent streams, with identical requests across providers:

- account updates and transactions, each at **processed** and **finalized** commitment,
- one slot stream containing all lifecycle statuses, including processed, confirmed and finalized.

Slots are keyed by `(slot, status)`. Their single stream is stored under the existing `processed` label, with `filter_by_commitment=false` and `interslot_updates=true`; it receives every status. Dashboards use this stream in either commitment view and exclude the retired duplicate stored under `finalized` from metrics, downtime and configuration selection. No schema migration is needed; historical duplicate rows remain in the database for direct SQL inspection.

Account streams can filter by `owner`; transaction streams can filter by `account_include`. Each list is limited to 100 entries; an empty list requires its own explicit `allow_all_accounts` or `allow_all_transactions` opt-in. These opt-ins only permit unrestricted requests and do not alter nonempty filters or measurement IDs. Account subscriptions require a transaction signature (`nonempty_txn_signature=true`) at both commitments, excluding updates without a modifying transaction, including ordinary startup snapshots.

On each subscriber, the same event is matched across providers. Each provider's delivery is recorded as a nanosecond lag behind the first observed arrival (the "ideal" provider). Matching uses a single configurable window, 5 seconds in the example configuration.

Out of scope: TCP network probes, validator probes, slot-leader data and WebSocket comparisons.

## Slot admission

A trusted RPC supplies independent `getSlot` references at processed, confirmed and finalized commitments. A new account or transaction comparison is admitted only within its commitment's reference ± `slot_validation.slot_buffer` (default 25 slots, inclusive). Slot notifications use the reference for their actual status: processed for lifecycle/dead statuses, confirmed for confirmed, and finalized for finalized. This avoids comparing live processed slots against an older finalized tip. Already-open comparisons can receive peers as their slots age below the lower boundary, so advancing the reference cannot invent a missed deadline; this exception never opens a new comparison or bypasses a reference outage.

Admission runs before matching or recording slot fates. Stale replays and future outliers cannot open comparisons or move fork-history pruning; only the fresh RPC finalized reference advances the pruning watermark. The guard checks plausibility, not ancestry or block contents. In-range provider fate reports are still trusted, and the reference RPC must be accurate and current.

No measurements are scored or certified until initial references arrive. An expired reference invalidates open comparisons and ends healthy coverage; recovery starts a new interval, including if a busy matcher observes only the recovered reference. Reference polling runs on the pipeline runtime without changing reader arrival stamps. See [Operations](operations.md#slot-reference) for defaults and failure behavior.

## Missed deadlines

**A provider records a missed deadline when it does not deliver an event within the comparison window, even if it delivers later.** This does not establish that the event was never delivered.

- The window opens at the first provider's delivery. A connected provider that has not delivered by the deadline is recorded as `missed`, even if its delivery arrives later. There is no separate "late" outcome.
- Only on-time deliveries enter lag percentiles, wins and head-to-head differences.
- Missed deadlines are only counted for a continuously connected, healthy stream. Incomplete intervals are unknown, not missed deadlines.
- After each successful subscription, a provider is eligible for missed deadlines (and healthy coverage begins) only after `connection_warmup` (default 1 second; `0s` disables it). A server may apply the filter after accepting the subscription, so events from just before it may be delivered by a slower provider that could never have sent them. Deliveries during the warmup still count, and downtime still uses the subscription time.
- A processed account update or transaction on a fork that never confirms is delivered only by providers that replayed that fork. A processed miss therefore waits for its slot's fate, as reported by any provider on the slot streams: in a confirmed or finalized slot it is a missed deadline (`missed`); in a dead slot, or a slot still undecided after `slot_confirmation_timeout` (default 30 seconds), it is recorded as `forked`. Finalizing a later slot does not settle an earlier one, because independent slot workers can process its confirmation later. Forked misses are kept for auditing but are neither missed deadlines nor competitors for wins.

## Late deliveries

After a window closes, its key is remembered for `expired_key_retention` (default 2 minutes). A delivery after the window is ignored, so it cannot start a new comparison in which the late provider appears first and the punctual providers are marked as missing the deadline.

Up to `expired_key_capacity` exact keys are kept per stream. The default is `matcher_key_capacity` × ⌈`expired_key_retention` / (`window` + `processing_grace` + 1 s)⌉, enough for the whole retention at the rate `matcher_key_capacity` is sized for. Whenever keys are forgotten, through normal expiry or capacity eviction, the highest such slot becomes a permanent floor for that process and any new key at or below it is ignored too. Already-open comparisons continue normally. Slot statuses keep a floor per status, since finalized trails processed. This bounded-memory safeguard prevents replay from reopening a comparison even after its exact digest expires, at the cost of excluding previously unseen keys at or below the floor.

## Configurations

The window, provider set, RPC reference URL and slot-validation policy are part of each configuration's ID. Lag behind ideal and win shares are relative to every configured provider, so changing the window, or adding, removing, renaming or re-pointing a provider (a different endpoint URL), starts a new configuration. The RPC admission guard isolates earlier measurements under different configuration IDs. Results measured under different definitions are never mixed.

Account identity is `(slot, pubkey, txn_signature)`, replacing the earlier first-update-per-account-and-slot measurement. The signature requirement and identity version start new histories for the two account streams; transaction and slot histories are unaffected. Account configuration labels include **per transaction** so historical results remain distinguishable.

Provider order and separate provider tokens are not part of the ID, so a rotated x-token continues the same configuration. Stored descriptions and dashboard labels list provider names but show endpoints, including the reference RPC, only as a short hash, since URLs can embed credentials. Changing credentials embedded in a URL changes that URL's hash and configuration ID.

## Limits

This is not an independent proof that every chain event was delivered.

- Accounts are matched by `(slot, pubkey, txn_signature)`. Different transactions updating the same account in one slot are separate events. Repeated deliveries or writes with the same tuple use only each provider's first delivery; node-local `write_version` is not compared across providers. Updates without a valid 64-byte transaction signature cannot open comparisons. Matching a transaction-linked account update does not independently verify its payload or ancestry.
- UTC is used for report time ranges; monotonic timestamps are used for differences. Timestamps from different subscribers are never subtracted.

## Message size

Account and transaction arrivals store `message_bytes`: the encoded size of that provider's full protobuf `SubscribeUpdate`, including metadata and the update envelope, excluding gRPC framing, HTTP/2, TLS and compression. Repeated deliveries of an account/transaction tuple do not add bytes; different transactions modifying the same account count separately. Missed events and slot updates have NULL sizes, and throughput charts leave gaps for buckets with unknown sizes.

## Coverage

A failed insert, full queue, expired RPC reference, restart or missing health evidence makes coverage **unknown**, not zero missed messages.

- Inserts use 10-second send and 30-second response timeouts, and the subscriber terminates on failure. The writer uses bounded memory, not a durable spool, and ClickHouse MergeTree does not enforce uniqueness after ambiguous retries.
- On shutdown, or when a task fails, matching stops first: pending certified coverage and undecided processed misses (as `forked`) are written, then accepted rows are flushed. Matchers have 60 seconds to drain; a timeout or task failure produces an unsuccessful exit and suppresses the final rollup pass. Windows still open are not written; no coverage interval contains them.
- Before inserting raw rows, the writer syncs pending minute IDs to its local recovery journal, then acknowledges per-minute expected-row counts in ClickHouse. A failed or partial insert leaves unmatched counts; on restart the journal also marks those minutes incomplete, including when the database intent itself failed. Affected minutes stay unknown and are excluded from summaries, dashboard raw fallback, and the coverage gate. Keep `state_dir` on persistent storage. The journal is not a row spool and cannot recover missing rows. Do not retry ambiguous inserts or write raw rows outside the subscriber.
- Each one-second tick processes everything already queued before closing windows, so a busy queue does not discard measurements. A full storage queue delays rows and ends the previous healthy interval at its last certification, even when capacity recovers before the next tick. Coverage resumes only after the blocked send completes and a clean tick drains the queues.
- A window is discarded only when its input may be incomplete: a reader stamped an update more than `processing_grace` ago without enqueuing it, an arrival was processed more than `processing_grace` after its stamp (at most one invalidation per tick), a provider gap, or a capacity overflow affecting that key. Such keys emit no lag classifications, even for providers already observed, and still expire on schedule. After a backlog, new healthy intervals start only once queues drain; the time in between stays unknown.

The raw example leaderboard and miss fraction are **not certified reliability claims**. For a reliability claim, use only event windows fully inside affirmative `healthy` `benchmark_measurement_health` intervals with no intersecting `unknown` gap. Missing coverage, including after crashes or outages, is unknown, not healthy.

Healthy coverage is written at most once a minute per provider and stream; pending certified time is written as soon as health is lost, so batching never drops it. It is certified only after draining controls and arrivals and after preceding ClickHouse inserts are acknowledged; neither a connected event nor the absence of gaps alone proves health.
