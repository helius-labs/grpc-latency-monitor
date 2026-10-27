# Grafana dashboards

Three dashboards are defined with the **Grafana Foundation SDK**, sharing builders in [`grafana/shared.ts`](../grafana/shared.ts) and ClickHouse SQL in [`grafana/queries.ts`](../grafana/queries.ts).

**Definitions used across dashboards:**

- **Ideal provider:** the first observed arrival for each event across all configured providers. Lag is milliseconds behind it, not chain-to-client latency.
- **Missed deadline:** not delivered within the configured comparison window after the first provider's delivery. Later delivery still counts as a missed deadline; this does not establish that the event was never delivered. The example window is five seconds; the selected configuration's label shows its actual window.
- **Win:** the first on-time delivery of an event that at least two connected providers competed for. A provider that misses an event’s deadline loses it to every provider that delivered it. First-place ties and uncontested deliveries are excluded; win shares divide wins by the selected providers' combined wins.
- **Downtime:** outages only, i.e. disconnected periods of at least 10 seconds in which reconnecting kept failing, counted in full. The threshold is `OUTAGE_SECONDS` in [`grafana/queries.ts`](../grafana/queries.ts). Events while a provider is disconnected are excluded from its comparisons rather than counted as missed deadlines.
- **Disconnects:** every interruption, including quick reconnects such as a rolling restart behind one endpoint.
- Disconnects caused by the subscriber's own backlog (`arrival queue full`, `control queue full`) are not provider downtime and are excluded.
- Missed deadline counts are raw observations. An empty missed deadline plot means no matching rows in the selected range, not certified zero missed deadlines.
- Minutes with unmatched raw-write counts are unknown and excluded from event metrics on both the summary and raw paths, including after a subscriber restart. Connection reporting remains available.

## All Providers

UID `solana-streaming-benchmark`. Source: [`grafana/providers.ts`](../grafana/providers.ts); JSON: [`grafana/solana-streaming-benchmark.json`](../grafana/solana-streaming-benchmark.json).

- Overview bars for win share and p50/p99 lag behind the ideal provider, plus **Downtime** and **Disconnects** bars.
- p50/p95/p99/p99.9 lag trends, win-share trends, observed event rate, missed deadline rate and message throughput. Throughput uses the same deduplicated on-time events as the event-rate chart.
- Shows every provider by default, with an optional Providers filter.

## Head to Head

UID `solana-streaming-head-to-head`. Source: [`grafana/head-to-head.ts`](../grafana/head-to-head.ts); JSON: [`grafana/solana-streaming-head-to-head.json`](../grafana/solana-streaming-head-to-head.json).

Compares two different providers, A and B. Negative A − B means A arrived earlier on the same event.

- Cards for p0.1/p1/p5/p50/p95/p99/p99.9 and average arrival difference over the selected range, both providers' win rates, and each provider's downtime.
- Graphs of average and p1/p5/p50/p95/p99 differences over time, win shares and missed deadline rates.
- Difference panels use only events delivered on time by both providers, including tied arrivals; they show "No data" when there are no eligible pairs.
- Win rates also count an on-time delivery as a win when the other provider misses the deadline. Ties are excluded, so the two rates sum to 100% whenever there is at least one win; with no wins they show "No data", and graph buckets with only ties remain gaps.
- The missed deadline graph includes unpaired events.

## Overview

UID `solana-streaming-overview`. Source: [`grafana/overview.ts`](../grafana/overview.ts); JSON: [`grafana/solana-streaming-overview.json`](../grafana/solana-streaming-overview.json).

One heat map per metric, with a row per location (region, plus cloud provider where a region has several) and a column per provider, for comparing providers across regions at a glance.

- **Performance** tab: win rate and p50/p99 lag behind the ideal provider.
- **Reliability** tab: missed deadlines, downtime and disconnects, measured as on All Providers.
- Providers are compared only within a location; a provider not running at a location shows `—`.
- Reads only committed [minute summaries](../sql/README.md#hybrid-dashboard-summaries), so it stays fast over long ranges but does not show the newest, still-open minute.
- Defaults to the last hour, refreshes every minute; select stream and commitment.

All Providers and Head to Head default to the last 30 minutes and refresh every 30 seconds. Connection transition annotations appear on every trend graph; toggle **Connections** (turquoise) and **Disconnections** (red) in **Controls**. Links between dashboards preserve the time range and shared filters.

## Selecting configurations

In All Providers and Head to Head, select region, cloud provider, stream and commitment. The subscription defaults to **Latest (automatic)**: on every refresh, each data type uses the subscription with the most recent observation within that scope and time range. Configurations remain separate, even when the range spans a filter change, and a range without observations shows no event metrics. For connection reporting, streams without observations fall back to the latest connection history within the preceding day and selected range, so an entire outage remains visible.

- The **Configurations observed in this time range** table lists readable labels for every configuration in the range; graphs show only the selected one for each stream.
- To inspect an older configuration, open **Controls → Subscription history** and choose its label and last-observed UTC timestamp. Select **Latest (automatic)** to follow configuration changes again. A bookmarked URL with a specific `var-subscription_id` keeps that selection until switched back.
- Expand **Subscription details** at the bottom for full filter lists and subscription IDs.

## Combining streams

Choose **All** in the Stream selector to combine account, transaction and slot events on either dashboard.

- Percentiles use the pooled event samples rather than averaging stream percentiles, and win rates use the pooled wins, so streams with more events contribute more.
- Event rates sum across streams. Message throughput combines account and transaction bytes; slots contribute none.
- All always selects the latest configuration separately for each stream. To inspect a historical configuration, select an individual stream first; the subscription details table identifies each configuration's stream.

## Building and deploying

To edit and rebuild the dashboards, use Node.js 22 or newer. Edit the SDK source and rebuild the JSON before committing; keep the lockfile for reproducible installs.

```sh
npm --prefix grafana ci
npm --prefix grafana run check
npm --prefix grafana run build
```

Deployment uses the [gcx](https://github.com/grafana/gcx) CLI (Grafana 12 or newer). Point a context at your Grafana instance and deploy with credentials from your environment:

```sh
gcx config set stacks.bench.grafana.server https://grafana.example.com
gcx config set stacks.bench.grafana.org-id 1
gcx config set contexts.bench.stack bench
gcx config use-context bench
export GRAFANA_USER=admin
read -rs GRAFANA_PASSWORD && export GRAFANA_PASSWORD
npm --prefix grafana run deploy
```

Deployment creates or updates all three dashboards by their stable UIDs, using gcx's current context. `GRAFANA_SERVER` overrides its server, and `GRAFANA_TOKEN` can replace username/password authentication.

To install manually, use **Dashboards → New → Import** and upload each generated JSON.

For a local trial, `docker compose up` starts ClickHouse and a Grafana with the data source and the generated dashboards provisioned, at <http://localhost:3000> (anonymous viewer access; sign in as `admin`/`admin` to edit). Provisioned dashboards use the generated JSON as is, so the Overview shows rows instead of the tabs that `deploy` creates.

## Data source

The dashboards need a ClickHouse data source with UID `benchmark-clickhouse` (override with `CLICKHOUSE_DATASOURCE_UID` when building, or edit the UID in the JSON) and read access to your benchmark database.

**The benchmark database can have any name.** Dashboard SQL uses unqualified table names, which ClickHouse resolves in the data source's **Default database**. Set that to the database subscribers write to (`clickhouse.database`); no rebuild is needed. A data source whose default database is something else fails with `UNKNOWN_TABLE`, so use a dedicated data source for the benchmark. The dashboard build rejects database-qualified table names.

## Implementation notes

- Panels that can span long ranges have two forms in [`grafana/queries.ts`](../grafana/queries.ts): an exact one over raw rows and one over committed minute summaries. Each refresh picks one from the time range and bucket size (see [Hybrid dashboard summaries](../sql/README.md#hybrid-dashboard-summaries)). Minutes not summarized yet are rebuilt from raw rows with the subscriber's own summary SQL, [`sql/rollups/build.sql`](../sql/rollups/build.sql).
- `grafana/clickhouse.ts` implements the SDK's query extension point, because the SDK has no ClickHouse builder.
- Panels share query results: Head to Head uses one query for all ten stat cards and one for both comparison graphs; All Providers shares its percentile/rate trends and its p50/p99 overview results. Consumers use Grafana's built-in Dashboard data source with the source panel's untransformed results.

## Tests

`npm --prefix grafana test` runs the generated dashboard SQL in `clickhouse-local` against small fixtures (it needs Python 3 and the [`clickhouse`](https://clickhouse.com/docs/install) CLI). `npm --prefix grafana run test:rollups` checks that summaries and raw rows give the same results on a real ClickHouse server; see [CONTRIBUTING.md](../CONTRIBUTING.md).
