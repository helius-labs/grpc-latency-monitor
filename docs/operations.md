# Operations

Installing, configuring and running a subscriber. To try it locally first, see the [quick start](../README.md#quick-start).

## Requirements

- Rust (see `rust-version` in `Cargo.toml`).
- Yellowstone gRPC endpoints that permit the configured subscriptions. Empty account or transaction filter lists request all messages of that type, which can have substantial bandwidth and ClickHouse storage cost; size and load-test before production.
- ClickHouse with a database created for the benchmark, and a user authorized to insert into its tables.

## Configuration

Copy `config.example.yaml` to a local file (`*.local.yaml` and `*.local.yml` are ignored by Git) and set:

- account `owner` and transaction `account_include` public-key lists,
- provider endpoint URLs, and environment-variable **names** for their credentials,
- subscriber `region`, `cloud_provider`, `latitude` and `longitude` (all required).

The example contains no working endpoints or secrets. Run one instance and configuration per `(region, cloud_provider)` pair. Filter lists are sorted and deduplicated before building the same request for every provider; changing a list changes that stream's subscription ID, keeping historical comparisons separate.

Latitude and longitude are your subscriber's city or datacenter coordinates; missing, null, non-finite or out-of-range values prevent startup. For example:

```yaml
subscriber:
  region: "FRA2"
  cloud_provider: "example-cloud"
  latitude: 50.1109
  longitude: 8.6821
```

## Credentials

The benchmark loads `.env` from the current working directory before reading configuration. Copy `.env.example` to `.env` and set your ClickHouse credentials and provider tokens there. Already exported environment variables take precedence. A missing `.env` is allowed; an unreadable or malformed file causes a startup error. Never put provider tokens in log messages.

## ClickHouse

Set `clickhouse.url` to the external ClickHouse HTTP(S) endpoint (typically port 8123, not the native port 9000). Create the database and apply the [Goose migrations](../sql/README.md) with a separate migration user. The application user needs:

- INSERT and DESCRIBE on the five ingestion/metadata tables and `benchmark_dirty_minutes`,
- SELECT on event lags, dirty minutes and rollup commits,
- INSERT on minute rollups and rollup commits,
- ALTER DELETE on minute rollups (to delete superseded summary generations).

Retention is managed in the SQL migrations: six hours of raw measurements and 30 days of minute summaries and connection/health history. Each subscriber builds its own [minute summaries](../sql/README.md#hybrid-dashboard-summaries) in the background; no separate rollup service is needed.

`state_dir` defaults to `.benchmark-state` in the working directory. Keep it on persistent storage and mount it into subscriber containers. It stores only pending minute IDs, under filenames hashed from database and subscriber identity. The writer syncs this recovery metadata before attempting raw inserts and clears it after all batch inserts are acknowledged. Startup marks any remaining minutes unknown in ClickHouse before starting rollups or provider subscriptions; if recovery fails, startup stops. Preserve the directory when restarting or moving the subscriber.

## Running

```sh
cargo run --release -- --config your.local.yaml
```

Startup checks, using the Rust row structs, that the written columns exist and are insertable before opening provider streams. This metadata check does not test INSERT privileges; see [Startup validation](../sql/README.md#startup-validation).

Allow at least 60 seconds for matchers to drain, the writer's remaining insert timeouts, and 150 seconds for final rollups when setting termination grace. A matcher timeout or failed writer produces an unsuccessful exit; only a clean ingestion shutdown publishes the final open minute. Alert on unmatched expected-row counts or a positive `incomplete` count in `benchmark_dirty_minutes`: affected minutes stay unknown after restart and require investigation, not blind insert retries.

## Tuning and connection behavior

Stream readers run on their own Tokio runtime, separate from matching, the ClickHouse writer and summaries, so that work never delays a timestamp.

Configurable: `window`, `processing_grace`, `expired_key_retention`, `expired_key_capacity`, `connection_warmup`, `slot_confirmation_timeout`, `matcher_key_capacity`, queue sizes, batch size and flush interval. Set `window` to the latest delivery you still consider on time. Size `matcher_key_capacity` for at least your peak unique events per second per stream × (`window` + `processing_grace` + 1 second). Keys beyond the capacity are dropped (and remembered), with one unknown-coverage marker per second.

Connections:

- Adaptive HTTP/2 receive windows let flow control adjust to connection throughput and latency.
- HTTP/2 keepalive pings (every 10 seconds, 10-second acknowledgement timeout) detect dead connections. The reader also pings every 10 seconds and reconnects a stream that receives no update, ping or pong for 30 seconds.
- Subscriptions time out after 10 seconds. Messages up to 64 MiB are accepted (tonic's default is 4 MiB; accounts can hold up to 10 MiB).
- A subscription that stayed up for 30 seconds reconnects immediately after it drops, unless the disconnect was caused by the subscriber's own backlog. Consecutive failures back off from 1 second, doubling up to 60 seconds, so a long outage is not retried constantly; its recorded end can lag the provider's recovery by up to a minute.

## Reports

See `sql/reports.sql` for sample head-to-head, leaderboard, connectivity, regional and coverage queries. In a head-to-head comparison, negative `lag_A − lag_B` means provider A arrived first. Do not calculate this by subtracting separately aggregated medians.
