# Accuracy

How much measurement noise the benchmark itself adds, and how to tell a real provider difference from that noise. All timing values are in milliseconds (ms).

## Methodology

Three tests, each comparing a source against itself so that the expected difference is zero:

1. **Matcher:** every update from a live subscription is fed to the matching loop twice, as providers A and B with an identical arrival stamp. Any nonzero lag, drop or unpaired event is an error in the matcher.
2. **Reader:** one live subscription per stream is relayed to two independent readers. The difference between their arrival stamps for the same event is the reader's own jitter.
3. **A/A control in production:** the same provider endpoint is configured twice under two names on a production subscriber, each with its own connection, alongside the other providers. This measures the full end-to-end noise, including how the provider schedules two connections.

Tests 1 and 2 used the subscriber's own code paths (readers, matching loop, ClickHouse writer and summaries) with the production runtime layout. All tests used Helius as the source, PumpSwap filters (accounts owned by and transactions including `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`), `window: 5s` and `processing_grace: 2s`.

An arrival is stamped when the gRPC stream yields a message, after it has been received and fully decoded. Every provider is stamped the same way, so comparisons stay fair, but decode time grows with message size, so large account updates are stamped slightly later than their bytes arrived.

## Results

### Test #1: Matcher (2026-10-09)

| Run | Pairs | Nonzero lags | Dropped | Unpaired | Gaps | Disconnects |
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

- **Clean runs:** no drops, gaps or disconnects.
- **No reader bias:** signed medians were within ±0.01 ms, and reader A won 48–51% of decided pairs.
- **Finalized transactions are the noisiest:** they arrive in bursts of roughly 280 messages per finalized slot, and draining a burst dominates the tail.

The relay is included in these numbers, so they are upper bounds for reader jitter alone.

### Test #3: A/A control in production (2026-10-15)

About 36 minutes on a production subscriber. All six streams stayed connected and healthy, with no gaps or reconnects, and both copies dropped exactly the same events. Exact percentiles over every event both copies delivered on time:

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
- **Startup drops:** in the first few milliseconds after a restart, one provider can deliver events the others never send, since each subscription starts at a slightly different point on the chain. These count as drops for the other providers.

## How to read head-to-head results

- **Medians and averages over many events:** a stable offset larger than ±0.01 ms is a real difference.
- **Single events and tails (p95, p99):** compare against the Test #3 spread for the same stream and percentile.
- **Win rates:** treat anything within about ±2 points of 50% as a tie, and ignore finalized-stream win rates over ranges of a few minutes.

The maxima above are observed samples, not upper bounds on future jitter.
