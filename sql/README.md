# ClickHouse migrations

Goose owns schema changes. The Rust application validates the columns it writes at
startup; it does not create tables, apply migrations, or change retention.
`migrations/` is the source of truth. `reports.sql` contains example queries.
All benchmark tables, including Goose's tracking table, use the `benchmark_` prefix
so they can coexist with other applications in the same database.

## Install and apply

Install [Goose](https://github.com/pressly/goose) with ClickHouse support. These
migrations are validated with v3.28.0:

```sh
go install github.com/pressly/goose/v3/cmd/goose@v3.28.0
```

Create the database first using an administrator (for example,
`CREATE DATABASE IF NOT EXISTS benchmark`). From the repository root, configure
Goose through environment variables or your local, ignored `.env` file:

```sh
export GOOSE_DRIVER=clickhouse
export GOOSE_MIGRATION_DIR=sql/migrations
# Local native protocol example; use migration credentials for your deployment.
export GOOSE_DBSTRING='clickhouse://migration_user:password@localhost:9000/benchmark'
# TLS native protocol example (URL-encode special characters in credentials):
# export GOOSE_DBSTRING='clickhouse://migration_user:password@host:9440/benchmark?secure=true&skip_verify=false'

goose -table benchmark_goose_db_version validate
goose -table benchmark_goose_db_version status
goose -table benchmark_goose_db_version up
goose -table benchmark_goose_db_version status
cargo run -- --config your.local.yaml
```

Goose's native endpoint (usually 9000, or 9440 with TLS) differs from the Rust
application's HTTP(S) endpoint. Both must target the same database. Supply actual
credentials privately; do not commit them. Goose stores migration history in
`benchmark_goose_db_version` in that database. Always pass
`-table benchmark_goose_db_version` to database commands so they use the same
history. Run one benchmark migration process per database as a deployment step,
before starting subscribers. The migration user needs privileges
for migration DDL and reading/writing Goose's history table. The application user
needs INSERT and DESCRIBE on the five ingestion/metadata tables and dirty minutes, SELECT on event
lags, dirty minutes and rollup commits, INSERT on minute rollups and rollup
commits, and ALTER DELETE on minute rollups (lightweight deletes of superseded
summary generations). It does not need other DDL privileges or access to Goose's history.

The initial migration uses plain `CREATE TABLE`: if any target benchmark table
already exists, migration fails instead of adopting or skipping it. After a
successful migration, subsequent `goose up` runs skip it using Goose's history.
Because ClickHouse DDL is not transactional, a collision with a later table can
leave earlier tables created and the migration unapplied. Inspect and resolve the
partial state before retrying; the migration never drops conflicting tables.

## Adding schema changes

Create a new numbered migration from the repository root:

```sh
goose -table benchmark_goose_db_version -dir sql/migrations -s create add_event_metadata sql
```

Put `-- +goose NO TRANSACTION` at the top and SQL after `-- +goose Up`; finish each
statement with a semicolon. ClickHouse schema migrations are not atomic: earlier
statements can remain applied if a later statement fails. Use retry-safe DDL where
possible and inspect partial failures before retrying. Do not edit applied
migrations; add a new one. Our migrations are forward-only: correct mistakes with
another migration rather than dropping production data through `down` or `reset`.
The initial migration explicitly rejects rollback.

Test on a disposable database, including a second
`goose -table benchmark_goose_db_version up` and an upgrade from the preceding
schema. Add compatible columns before deploying code that uses them;
remove obsolete columns only after every writer and query has stopped using them.
Plan table replacement/backfill separately for changes that require rewriting data.
Update the writer row definitions in `src/storage.rs` when the application starts
using changed columns; startup validation uses those same structs.

## Retention

After all migrations, installations retain raw event measurements for six hours, minute summaries, connection events and health history for 30 days, and dirty-minute markers for 31 days. `benchmark_subscription_versions` and `benchmark_subscriber_locations` have no TTL.
To change retention, add a migration for the relevant tables. Dirty-minute markers
must outlive minute summaries: a summary whose marker expired first would become valid
again even if it was never rebuilt.

Raw measurements use hourly partitions with `ttl_only_drop_parts`, so expiry drops
whole parts instead of rewriting them; expired hours can remain up to about an hour
longer before their part is dropped. `event_id` is a `UInt128` (the first 16 bytes
of the event's BLAKE3 identity) rather than a 64-character hex string.
Account identities include `(slot, pubkey, txn_signature)` with their own hash
domain; different transactions modifying one account in a slot have different
stored IDs. The account key change uses new subscription IDs and needs no schema
migration. Transaction and slot event IDs retain their existing encoding.
Adjust the worker safety window and dashboard routing if raw retention changes. For example, to extend summary history:

```sql
-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE benchmark_minute_rollups MODIFY TTL bucket + INTERVAL 60 DAY DELETE;
ALTER TABLE benchmark_rollup_commits MODIFY TTL bucket + INTERVAL 60 DAY DELETE;
ALTER TABLE benchmark_dirty_minutes MODIFY TTL bucket + INTERVAL 61 DAY DELETE;
```

Retention is a database policy managed by migrations, not application YAML.
Changing it can trigger work on existing data; lowering it can delete older rows.

## Startup validation

Before opening provider streams or starting the writer, the application prepares
an insert for each existing Rust row type using the driver's metadata validation,
with a 30-second deadline for the whole check. Preparation runs `DESCRIBE TABLE`,
checks derived column names against the table, and caches metadata. The insert
handles are dropped without writing rows or finalizing inserts, so startup sends
no INSERT requests and changes no data. There is no separate expected-schema list.

Missing tables or required columns, required columns defined as ALIAS or
MATERIALIZED, and additional columns without defaults cause startup to fail with
table context and a migration hint. Additional DEFAULT, ALIAS, or MATERIALIZED
columns are allowed. Add an explicit DEFAULT when introducing a column that older
writers omit, so the driver's metadata validation permits rolling upgrades.

Type compatibility is checked by the driver when it serializes actual rows;
startup preparation does not validate Rust field types against ClickHouse types.
This check does not validate Goose's version number, engines, sorting/partition
keys, codecs, TTLs, INSERT permissions, or data contents. Metadata is cached and
cannot prevent schema changes made after startup. Restart subscribers after schema
changes so they fetch fresh metadata, and keep DDL restricted to the migration user.

## Subscriber identity

The initial schema uses `(region, cloud_provider)` as subscriber identity in all
three fact tables and their sorting keys. Run one subscriber per pair.
`subscriber.cloud_provider` is required in the application configuration.

The undeployed schema is consolidated into `00001_initial_schema.sql`; it is edited
in place rather than extended with new migrations until the benchmark is deployed.
When recreating a development installation that used an earlier version, stop the
subscribers, drop all benchmark tables and views and the `benchmark_goose_db_version`
tracking table, then run `goose up` so Goose applies the current initial migration:

```sql
DROP VIEW IF EXISTS benchmark_dirty_minutes_mv;
DROP TABLE IF EXISTS benchmark_event_lags, benchmark_connection_events,
  benchmark_subscription_versions, benchmark_measurement_health,
  benchmark_subscriber_locations, benchmark_minute_rollups, benchmark_dirty_minutes,
  benchmark_rollup_commits, benchmark_goose_db_version;
```

## Subscriber locations

The initial migration creates `benchmark_subscriber_locations`. Grant the
application user INSERT and DESCRIBE access to this table. Required latitude and longitude are stored as non-null Float64 columns and
appended once per startup. Latest records are selected by `(region, cloud_provider)`. The table has no TTL.

The map computes win shares independently for each subscriber using the latest
observed subscription per stream within the selected time range and commitment.
All streams pools their events into one marker per subscriber. It
does not combine clocks or events across locations. Locations without observations
in the time range are absent, including when the metadata table contains them.

## Hybrid dashboard summaries

The initial migration creates minute summaries and subscriber-scoped rollup tracking.
Raw measurements retain **six hours**; summaries, connection events and health
history retain **30 days**, and dirty-minute markers **31 days**. A
minute still dirty when its raw rows expire stays excluded rather than showing a
stale summary. Apply it to an empty database before
starting subscribers. Lowering retention in future migrations can expire older rows.

Each subscriber includes a Rust background rollup task, scoped to its configured
**region + cloud provider**. Run one subscriber per combination across all hosts.
No extra service, environment file or rollup configuration is required. Dirty markers
and publication manifests are scoped to the owning subscriber.

The task polls every ten seconds using the subscriber's existing ClickHouse client
and credentials. ClickHouse executes the aggregations; the subscriber schedules
and publishes them. Rollup errors are logged and retried independently of ingestion.
A local lock rejects duplicate owners on the same host; deployment must enforce
uniqueness across hosts. On Ctrl-C or SIGTERM, the subscriber flushes accepted raw
records and then summarizes remaining work, including the current open minute
(with a 150-second timeout). Allow sufficient termination grace in deployment.
After a crash, the next startup catches up on retained, eligible dirty minutes.

The subscriber's ClickHouse user needs SELECT on raw, dirty-minute and commit
tables, INSERT on minute summaries and commits, and ALTER DELETE on minute summaries,
in addition to its existing writer permissions, including INSERT and DESCRIBE on dirty minutes. The dirty-minute view must also be allowed to write its target
under the deployment's ClickHouse view security policy. Configuration is unchanged.

The worker recomputes a complete event-time minute from raw rows. It deduplicates
provider/event observations and applies the win rules: a win is the first on-time
delivery among at least two competing providers (delivered on time or missed the deadline while connected),
excluding ties. It stores both ordered provider-pair orientations for every event both
providers competed for: differences use events both delivered on time, and an on-time
delivery counts as a pair win when the other provider misses the deadline. It never pairs records within an
individual INSERT block. A new immutable generation becomes visible only after
its manifest is inserted successfully. Failed builds remain invisible; a retry
uses a new generation. Every ten minutes the worker deletes generations that are not
the latest committed one for their minute (superseded builds and orphans of failed
builds), once both they and the minute's latest commit are at least ten minutes old,
so a dashboard query that captured the previous manifest can still finish. Rows are written when an event's window closes (window + processing grace after
its first arrival), so the last seconds of a minute arrive after it ends; those inserts
mark the event-time minute dirty and trigger a rebuild.

Dirty revisions are cumulative inserted-row counts, summed by `SummingMergeTree`,
so even a one-row insert after a larger published revision invalidates it. Revisions
do not use wall clocks. Before each raw insert, the writer inserts and acknowledges
the expected number of rows for every affected minute. When expected rows exceed
inserted rows, that minute is incomplete and stays excluded from summaries, raw
dashboard fallback and the coverage gate across restarts. The counters outlive the
summaries they invalidate. A subsequent successful batch adds equal counts to both
sides and cannot repair a previous deficit. Only the subscriber may write raw rows;
manual inserts or blind retries could conceal a deficit. Missing rows cannot be
recovered by rebuilding a summary.

Before sending the expected-row insert, the writer syncs pending minute IDs to
its local `state_dir` journal. Startup marks any journaled minutes with a sticky
`incomplete` count before starting ingestion or rollups, then clears the journal
only after that marker is acknowledged. This also covers failure before the
database intent is acknowledged. Preserve `state_dir` across restarts; see
[operations](../docs/operations.md). A positive `incomplete` count excludes a minute
regardless of its row counts. Live batch tracking adds one small intent INSERT
and two local file syncs to each batch containing raw rows.

Dashboard queries capture one manifest snapshot. Unbuilt/dirty minutes and partial
range boundaries use raw rows; committed fully contained minutes use summaries.
This also preserves latest-subscription selection separately for every stream and
location. Normal retries retain the same event ID and event timestamp; rewriting
historical timestamps or manually mutating raw rows requires an explicit rebuild.

A closed minute is first built after a settle delay of `window + processing_grace +
flush_interval + 6 s` (14 seconds with the example configuration): the last windows of
a minute close and are inserted after it ends, so building sooner would rebuild almost
every minute and retain the superseded generation for 30 days. At shutdown, after the
writer and matchers finish successfully, all complete minutes including the open
one are built immediately. Failed ingestion suppresses that final pass; incomplete
minutes remain excluded on later startups as well.

During normal operation, the task builds closed minutes newer than five hours to avoid
racing raw TTL deletion during a long build. **Alert well before worker lag reaches
one hour**; a prolonged outage can permanently lose summaries when raw data expires.
A query falls back to available raw data while the worker catches up, so performance
can temporarily degrade. Data already removed by TTL cannot be recovered.

Overview panels use exact raw percentiles for ranges up to one hour. Larger ranges
use t-digest states. Charts use exact raw data for sub-minute buckets in ranges up to six hours, and merge
summaries for longer ranges or minute-aligned buckets of at least 60 seconds.
Longer ranges automatically round the requested bucket size up to a whole minute. The chart minimum is
ten seconds, with 2,160 points so a six-hour range can display ten-second buckets.
Counts, win shares, byte totals and signed means remain exact; percentile sketches
are approximate and are merged, **never averaged**. Older raw detail cannot be
reconstructed from the summaries; historical windows older than six hours automatically
use minute-or-larger buckets, even when zoomed to a short range. A partial minute at
either edge of such a range uses its whole-minute summary once its raw rows have expired,
so the range is widened to whole minutes instead of silently omitting the edges. Connection/health records retain 30 days. A live six-hour range has a one-minute
routing tolerance for request latency; expired raw rows are never reconstructed.
Six-hour detailed charts still scan raw events and may be slower than summary views;
the reader memory/time limits continue to apply.

### Recommended Grafana user and resource limits

We recommend a dedicated, SELECT-only ClickHouse user for Grafana with explicit
resource limits, especially when ClickHouse also serves other applications.
Several dashboard panels and viewers can run queries concurrently. Limiting their
memory, threads, concurrency and runtime reduces the risk of dashboard queries
exhausting ClickHouse resources and disrupting ingestion or other workloads.
Keep this account separate from the subscriber and migration accounts.

Create the user manually with an administrator account. In the following example,
replace the password and size the limits for your server
and the capacity needed by other workloads:

```sql
CREATE USER benchmark_grafana
IDENTIFIED WITH sha256_password BY 'REPLACE_WITH_A_STRONG_PASSWORD';
GRANT SELECT ON benchmark.* TO benchmark_grafana;
ALTER USER benchmark_grafana SETTINGS
    readonly = 2,
    max_threads = 16 MAX 16,
    max_memory_usage = 6000000000 MAX 6000000000,
    max_memory_usage_for_user = 16000000000 MAX 16000000000,
    max_concurrent_queries_for_user = 12 MAX 12,
    max_execution_time = 60 MAX 60,
    max_bytes_before_external_group_by = 2000000000 MAX 2000000000,
    max_bytes_before_external_sort = 1000000000 MAX 1000000000;
```

`readonly = 2` permits query settings while preventing writes; the account also
receives only SELECT privileges. The `MAX` constraints prevent queries from
raising these settings above the configured caps. The example allows 6 GB per
query, 16 GB aggregate query memory for this user, 16 threads per query, 12 concurrent
queries and 60 seconds execution time. Aggregation can spill to disk after 2 GB,
and sorting after 1 GB. These are example limits, not a reservation or a guarantee
of isolation: shared CPU, disk and background work still need capacity. Limits may
reject expensive or concurrent requests rather than queue dashboard viewers.
Dashboard SQL also specifies query settings; if you choose lower caps, adjust the
settings in `grafana/queries.ts` to stay within them.

Configure a Grafana ClickHouse datasource with the server address, **Default database**
set to your benchmark database (`benchmark` here, but any name works: dashboard SQL uses
unqualified table names resolved in this database), username `benchmark_grafana` and the
password you created. Replace `benchmark.*` in the `GRANT` above if you chose another name. Use the
correct address and protocol from Grafana's network (including Docker networking
and TLS as applicable), and set the query timeout to 45 seconds. Store the password
in Grafana's secure datasource configuration, not dashboard JSON or this repository.

The generated dashboards reference datasource UID `benchmark-clickhouse` by default.
Use that UID, or set `CLICKHOUSE_DATASOURCE_UID` to your datasource's actual UID when
building/deploying the dashboards:

```sh
CLICKHOUSE_DATASOURCE_UID=your_datasource_uid npm --prefix grafana run deploy
```

This setup is recommended, not a requirement of the rollup implementation. An
existing datasource can be used with the same UID override. No setup script or
additional running service is required.

### Verification and monitoring

```sh
npm --prefix grafana run check
npm --prefix grafana test
# Real-server integration; creates and finally drops a uniquely named test DB:
npm --prefix grafana run test:rollups
```

Monitor subscriber rollup logs, query failures and `system.query_log.memory_usage`.
To find unprocessed closed minutes:

```sql
SELECT d.region, d.cloud_provider, d.bucket, d.revision, d.expected,
       d.expected > d.revision OR d.incomplete > 0 AS incomplete, c.revision AS published_revision
FROM (SELECT region, cloud_provider, bucket, sum(revision) revision, sum(expected) expected,
      sum(incomplete) incomplete FROM benchmark_dirty_minutes GROUP BY region, cloud_provider, bucket) d
LEFT JOIN (SELECT region, cloud_provider, bucket, argMax(revision, generation) revision FROM benchmark_rollup_commits GROUP BY region, cloud_provider, bucket) c
ON d.region = c.region AND d.cloud_provider = c.cloud_provider AND d.bucket = c.bucket
WHERE d.bucket < toStartOfMinute(now()) AND (d.revision > c.revision OR d.expected > d.revision OR d.incomplete > 0)
ORDER BY d.bucket;
```

Pair summary storage grows quadratically with provider count (both orientations).
Backfill capacity and memory limits should be re-measured when adding many providers
or subscriber locations. Summary generations increase write/storage work in exchange
for avoiding repeated raw reconstruction on every dashboard refresh.
