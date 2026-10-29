# Accuracy

How much measurement noise the benchmark itself adds, and how to tell a real provider difference from that noise. All timing values are in milliseconds (ms).

## Methodology

Three tests, each comparing a source against itself so that the expected difference is zero:

1. **Matcher:** every update from a live subscription is fed to the matching loop twice, as providers A and B with an identical arrival stamp. Any nonzero lag, missed deadline or unpaired event is an error in the matcher.
2. **Reader:** one live subscription per stream is relayed to two independent readers. The difference between their arrival stamps for the same event is the reader's own jitter.
3. **A/A control in production:** the same provider endpoint is configured twice under two names on a production subscriber, each with its own connection, alongside the other providers. This measures the full end-to-end noise, including how the provider schedules two connections.

Tests 1 and 2 used the subscriber's own code paths (readers, matching loop, ClickHouse writer and summaries) with the production runtime layout. All tests used Helius as the source, PumpSwap filters (accounts owned by and transactions including `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`), `window: 5s` and `processing_grace: 2s`.

These historical runs used six streams and the earlier first-update-per-account-and-slot matching. The current subscriber compares transaction-linked account updates and uses five streams, with one slot subscription covering every status. The figures below describe the tested historical setup.

An arrival is stamped when the gRPC stream yields a message, after it has been received and fully decoded. Every provider is stamped the same way, so comparisons stay fair, but decode time grows with message size, so large account updates are stamped slightly later than their bytes arrived.

## Results

### Test #1: Matcher (2026-10-09)

| Run | Pairs | Nonzero lags | Missed deadlines | Unpaired | Gaps | Disconnects |
|---|---:|---:|---:|---:|---:|---:|
| 1 (60 s) | 83,751 | 0 | 0 | 0 | 0 | 0 |
| 2 (60 s) | 73,905 | 0 | 0 | 0 | 0 | 0 |

**All 157,656 matched pairs reported exactly 0 ms**, with complete coverage on all six streams.

### Test #2: Reader (2026-10-09)

Two 60-second runs, **221,315 matched pairs**, absolute difference between the two readers. Each percentile is the higher of the two runs; the maximum is the largest sample across both.

| Data type | Commitment | Messages per run | p50 | p95 | p99 | p99.9 | Maximum |
|---|---|---:|---:|---:|---:|---:|---:|
| Accounts | Processed | ~21,900 | 0.012 | 0.043 | 0.086 | 0.708 | 2.330 |
| Accounts | Finalized | ~14,000 | 0.020 | 0.082 | 0.204 | 1.600 | 1.601 |
| Transactions | Processed | ~41,900 | 0.041 | 0.137 | 0.256 | 0.693 | 7.652 |
| Transactions | Finalized | ~40,200 | 0.104 | 0.847 | 1.373 | 6.096 | 6.288 |
| Slots | Processed | 228 | 0.011 | 0.032 | 0.063 | 0.341 | 0.341 |
| Slots | Finalized | 228 | 0.014 | 0.051 | 0.200 | 0.754 | 0.754 |

- **Clean runs:** no missed deadlines, gaps or disconnects.
- **No reader bias:** signed medians were within ±0.01 ms, and reader A won 48–51% of decided pairs.
- **Finalized transactions are the noisiest:** they arrive in bursts of roughly 280 messages per finalized slot, and draining a burst dominates the tail.

The relay is included in these numbers, so they are upper bounds for reader jitter alone.

### Test #3: A/A control in production (2026-10-15)

About 36 minutes on a production subscriber. All six streams stayed connected and healthy, with no gaps or reconnects, and both copies missed the deadlines for exactly the same events. Exact percentiles over every event both copies delivered on time:

| Data type | Commitment | Pairs | Signed median | p50 | p95 | p99 | p99.9 | Maximum | A wins |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Accounts | Processed | 366,199 | +0.0007 | 0.014 | 0.043 | 0.074 | 2.201 | 141.2 | 48.5% |
| Accounts | Finalized | 365,917 | −0.0007 | 0.065 | 0.705 | 1.070 | 5.696 | 201.5 | 50.6% |
| Transactions | Processed | 1,603,198 | +0.0014 | 0.030 | 0.123 | 0.205 | 0.626 | 5.4 | 48.2% |
| Transactions | Finalized | 1,602,787 | +0.0091 | 0.425 | 2.250 | 3.835 | 7.482 | 13.5 | 49.1% |
| Slots | Processed | 61,213 | 0 | 0.015 | 0.045 | 0.077 | 0.218 | 4.4 | 50.0% |
| Slots | Finalized | 61,213 | 0 | 0.017 | 0.303 | 0.788 | 1.325 | 48.7 | 50.0% |

The **p50–Maximum** columns are the absolute difference |A − B|; **A wins** is the first copy's share of untied pairs.

- **No meaningful bias:** signed medians were within ±0.01 ms on every stream.
- **Win rates near 50% are a tie:** processed streams leaned 51–52% toward the second copy, consistently but by microseconds.
- **Finalized streams are noisier and unstable over short ranges:** per 6-minute slice, the finalized account win rate ranged from 34.8% to 71.1%.
- **Rare spikes:** isolated differences reached 141–202 ms on account streams, but p99.9 stayed below 6 ms.
- **Startup missed deadlines:** in the first few milliseconds after a restart, one provider can deliver events the others never send, since each subscription starts at a slightly different point on the chain. These count as missed deadlines for the other providers.

## Current-code controls (2026-10-27)

Repeated the matcher and serialized-fanout reader controls against the current RPC-guarded, transaction-aware five-stream subscriber. Only Helius FRA (`https://laserstream-mainnet-fra.helius-rpc.com`) supplied gRPC data; the independent reference RPC was `https://api.mainnet.solana.com`, using the normal ±25-slot guard and freshness checks. Both tests retained full account payloads, the PumpSwap filters, a 5-second window, 2-second processing grace and 1-second connection warmup.

The local host used 46 reader threads and 2 pipeline threads, following the production runtime split. Reader tests added a separate 2-thread relay runtime. Results went through the production matcher, ClickHouse writer/recovery journal and minute-summary worker into disposable local ClickHouse 26.3 databases. No production database was used.

### Matcher

Each live event was stamped once, then sent twice to the current matcher with identical timestamps and different provider indices. The production request builder, account signature validation, slot admission and matching keys were used.

| Run | On-time pairs | Pairs inside certified coverage | Nonzero lags | Missed deadlines | Unpaired | Unknown health rows | Disconnects |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 (60 s) | 74,453 | 73,387 | 0 | 0 | 0 | 0 | 0 |
| 2 (60 s) | 74,160 | 72,577 | 0 | 0 | 0 | 0 | 0 |

**All 148,613 pairs had exactly zero lag.** Every stream had affirmative healthy coverage; 145,964 whole comparison windows fit within that coverage after excluding startup/shutdown boundaries.

### Reader

One live upstream subscription per stream fed a local gRPC relay. Each update was serialized once and the same protobuf bytes were broadcast to two independent connections running the unchanged production reader. Both downstream subscriptions registered before upstream delivery began. Relay sockets used `TCP_NODELAY`, matching normal tonic servers; reader startup order was reversed in run 2. Upstream load balancing cannot give the two copies different messages, because they share one upstream subscription per stream.

The two 60-second runs produced **124,462 matched pairs**, of which **122,232** whole comparison windows were inside affirmative coverage for both readers. There were no missed deadlines, forked outcomes, unpaired deliveries, unknown health rows, disconnects or relay receiver drops. All expected raw rows were acknowledged, and the summary workers flushed cleanly.

The following percentiles use only those certified pairs. Each percentile and maximum is the higher of the two runs, not a pooled percentile. All timing columns are milliseconds; signed medians are B lag minus A lag, so positive means A arrived first.

| Stream | Certified pairs, run 1 / 2 | p50 | p95 | p99 | p99.9 | Maximum | Signed median, run 1 / 2 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Account / processed | 12,985 / 13,653 | 0.010 | 0.045 | 0.094 | 0.318 | 0.574 | -0.000090 / +0.000361 |
| Account / finalized | 10,051 / 10,145 | 0.050 | 0.167 | 0.261 | 0.608 | 0.702 | +0.005360 / +0.000962 |
| Transaction / processed | 15,863 / 20,334 | 0.034 | 0.121 | 0.199 | 0.336 | 0.507 | -0.000160 / +0.000300 |
| Transaction / finalized | 15,968 / 20,287 | 0.157 | 0.727 | 1.442 | 2.541 | 2.663 | +0.017653 / +0.008356 |
| Slots / all statuses | 1,470 / 1,476 | 0.014 | 0.065 | 0.226 | 0.358 | 0.445 | +0.000951 / +0.000511 |

These are short local controls, including relay scheduling and transport overhead. They do not replace the historical direct production A/A control, establish a universal noise floor, or compare adaptive and fixed HTTP/2 windows. Finalized transactions remained the noisiest stream, with signed median offsets up to 0.017653 ms; the historical ±0.01 ms observation must not be assumed on every host.

The first live attempt also exposed a TLS initialization panic: ClickHouse enabled AWS-LC while the reference RPC client enabled Ring. ClickHouse now explicitly selects Ring with the same certificate roots; a regression test initializes ClickHouse, RPC and gRPC clients without opening a socket. Two initial relay transport controls showed roughly 40 ms tails with Nagle enabled; those diagnostic runs are excluded from the table above. Enabling `TCP_NODELAY` removed that tail, demonstrating why the relay's own transport settings matter.

## How to read head-to-head results

- **Medians and averages over many events:** compare offsets against an A/A control on the same subscriber host, filters, transport settings and a comparable traffic sample. The historical ±0.01 ms observation is a measured baseline, not a universal cutoff.
- **Single events and tails (p95, p99):** use the corresponding stream and percentile from a control matching the deployment. Test #3 includes production network/provider variation; the reader controls include the local relay.
- **Win rates:** calibrate against the same control rather than expecting exactly 50%. The historical production control was within about ±2 points, but short finalized-stream samples can vary substantially; do not infer a provider advantage from them alone.

The maxima above are observed samples, not upper bounds on future jitter.
