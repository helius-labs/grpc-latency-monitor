-- +goose NO TRANSACTION
-- +goose Up
-- Apply before starting subscribers. DateTime64(9, 'UTC') retains nanosecond UTC timestamps.
-- Raw measurements retain six hours; summaries and connection/health history retain 30 days.
-- MergeTree does NOT deduplicate retries.
-- event_id is the first 16 bytes of the event's BLAKE3 identity (binary, not hex):
-- it was ~70% of raw storage as a 64-character hex string. Rows sort by subscriber and
-- stream first, matching every dashboard and summary filter. Hourly partitions let TTL
-- drop whole expired parts instead of rewriting a daily partition.
CREATE TABLE benchmark_event_lags
(
    event_id UInt128 CODEC(ZSTD(1)),
    slot UInt64,
    data_type LowCardinality(String),
    commitment LowCardinality(String),
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    subscription_id LowCardinality(String),
    provider LowCardinality(String),
    event_utc DateTime64(9, 'UTC'),
    outcome LowCardinality(String), -- on_time | missed (dropped: not delivered within the window)
    lag_ns Nullable(UInt64) CODEC(ZSTD(1)), -- NULL only for missed
    message_bytes Nullable(UInt64) DEFAULT NULL CODEC(ZSTD(1)) -- NULL when unmeasured
)
ENGINE = MergeTree
PARTITION BY toStartOfHour(event_utc)
ORDER BY (region, cloud_provider, data_type, commitment, subscription_id, event_utc, event_id, provider, outcome)
TTL event_utc + INTERVAL 6 HOUR DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;

CREATE TABLE benchmark_connection_events
(
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    subscription_id LowCardinality(String),
    provider LowCardinality(String),
    data_type LowCardinality(String),
    commitment LowCardinality(String),
    event_utc DateTime64(9, 'UTC'),
    connected Bool,
    reason String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_utc)
ORDER BY (subscription_id, region, cloud_provider, provider, event_utc)
TTL event_utc + INTERVAL 30 DAY DELETE;

-- Six normalized request versions per writer startup. A version is immutable by ID;
-- configuration must use a new ID if filters/request change. Metadata has no TTL:
-- a continuously running writer can outlive fact retention without re-registering.
CREATE TABLE benchmark_subscription_versions
(
    subscription_id LowCardinality(String),
    data_type LowCardinality(String),
    commitment LowCardinality(String),
    filters String CODEC(ZSTD(3)),
    registered_utc DateTime64(9, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(registered_utc)
ORDER BY (subscription_id, data_type, commitment, registered_utc);

-- Periodic healthy rows certify only a short bounded interval for a connected stream after the
-- matcher has drained arrivals and controls, and the ordered writer has acknowledged
-- all preceding batches. Missing intervals are UNKNOWN (including process crashes).
-- Unknown rows are point markers. NULL provider = all providers in this stream.
-- No healthy row covering a period means unknown, including after a gap or crash.
-- Sink errors may not reach this same failed ClickHouse server: also alert on WriteError.
CREATE TABLE benchmark_measurement_health
(
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    subscription_id LowCardinality(String),
    provider Nullable(String),
    data_type LowCardinality(String),
    commitment LowCardinality(String),
    interval_start DateTime64(9, 'UTC'),
    interval_end Nullable(DateTime64(9, 'UTC')),
    status LowCardinality(String),
    reason String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(interval_start)
ORDER BY (subscription_id, region, cloud_provider, data_type, commitment, interval_start)
TTL interval_start + INTERVAL 30 DAY DELETE;

-- Append location metadata once per subscriber startup. Queries select the latest
-- coordinate tuple for each region/cloud provider. Coordinates are required.
CREATE TABLE benchmark_subscriber_locations
(
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    latitude Float64,
    longitude Float64,
    registered_utc DateTime64(9, 'UTC')
)
ENGINE = MergeTree
ORDER BY (region, cloud_provider, registered_utc);

-- Immutable generations are invisible until the worker publishes their manifest.
-- Keeping generations immutable also makes a failed/retried build safe.
CREATE TABLE benchmark_minute_rollups
(
    bucket DateTime('UTC'),
    generation UInt64,
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    subscription_id LowCardinality(String),
    data_type LowCardinality(String),
    commitment LowCardinality(String),
    kind LowCardinality(String), -- provider | pair (both ordered orientations)
    provider LowCardinality(String),
    other_provider LowCardinality(String),
    first_seen DateTime64(9, 'UTC'),
    last_seen DateTime64(9, 'UTC'),
    observations UInt64,
    delivered UInt64,
    missed UInt64,
    wins UInt64,
    losses UInt64,
    lag_sum Int128,
    byte_events UInt64,
    unknown_bytes UInt64,
    message_bytes UInt64,
    percentiles AggregateFunction(quantilesTDigestIf(0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999), Int64, UInt8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (region, cloud_provider, commitment, data_type, subscription_id, kind, provider, other_provider, bucket, generation)
TTL bucket + INTERVAL 30 DAY DELETE;

-- Dirty markers and publication manifests are owned by each region/cloud subscriber.
CREATE TABLE benchmark_dirty_minutes
(
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    bucket DateTime('UTC'),
    revision UInt64
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY (region, cloud_provider, bucket)
-- Markers must outlive the summaries they invalidate (30 days): if a marker expired
-- first, a summary that could never be rebuilt would silently become valid again.
TTL bucket + INTERVAL 31 DAY DELETE;

CREATE TABLE benchmark_rollup_commits
(
    region LowCardinality(String),
    cloud_provider LowCardinality(String),
    bucket DateTime('UTC'),
    generation UInt64,
    revision UInt64
)
ENGINE = MergeTree
ORDER BY (region, cloud_provider, bucket, generation)
TTL bucket + INTERVAL 30 DAY DELETE;

-- Inserts invalidate affected minutes, including rows written after the minute ended.
CREATE MATERIALIZED VIEW benchmark_dirty_minutes_mv TO benchmark_dirty_minutes AS
SELECT region, cloud_provider, toStartOfMinute(event_utc) AS bucket,
       toUInt64(toUnixTimestamp64Nano(now64(9))) AS revision
FROM benchmark_event_lags GROUP BY region, cloud_provider, bucket;

-- +goose Down
SELECT throwIf(1, 'Forward-only migration: create a corrective migration instead of rolling back');
