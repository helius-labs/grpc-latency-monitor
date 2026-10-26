-- Bind from_utc/to_utc (UTC DateTime64(9)), subscription_id, cloud_provider and region;
-- bind provider_a/provider_b for head-to-head. Ranges are [from_utc, to_utc).
-- Filters select event_utc (first local receipt); never subtract timestamps
-- from different subscribers. Each query stands alone. Duplicate identical rows
-- from ambiguous insert retries are collapsed per event/provider before aggregation.

-- HEAD-TO-HEAD: A minus B on the SAME event/region/cloud_provider/subscription;
-- negative difference means A was faster. Both must be on_time. Dashboard win rates
-- also count an event one provider delivered and the other dropped (missed) as a win.
WITH delivered AS
(
    SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id, provider,
           min(assumeNotNull(lag_ns)) AS lag_ns
    FROM benchmark_event_lags
    WHERE cloud_provider != '' AND event_utc >= {from_utc:DateTime64(9)}
      AND event_utc < {to_utc:DateTime64(9)}
      AND cloud_provider = {cloud_provider:String}
      AND region = {region:String}
      AND subscription_id = {subscription_id:String}
      AND outcome = 'on_time'
      AND provider IN ({provider_a:String}, {provider_b:String})
    GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id, provider
), pairs AS
(
    SELECT a.region, a.cloud_provider, a.subscription_id, a.data_type, a.commitment, a.event_id,
           toInt64(a.lag_ns) - toInt64(b.lag_ns) AS difference_ns
    FROM delivered AS a INNER JOIN delivered AS b
      ON a.region = b.region AND a.cloud_provider = b.cloud_provider AND a.subscription_id = b.subscription_id
     AND a.data_type = b.data_type AND a.commitment = b.commitment
     AND a.event_id = b.event_id
    WHERE a.provider = {provider_a:String} AND b.provider = {provider_b:String}
)
SELECT data_type, commitment, count() AS paired_events,
       countIf(difference_ns < 0) AS a_faster,
       countIf(difference_ns > 0) AS b_faster,
       countIf(difference_ns = 0) AS tied,
       quantileExact(0.5)(difference_ns) AS median_a_minus_b_ns,
       quantileExact(0.95)(difference_ns) AS p95_a_minus_b_ns
FROM pairs GROUP BY data_type, commitment ORDER BY data_type, commitment;

-- OBSERVED LEADERBOARD (NOT CERTIFIED RELIABILITY): speed is only on-time;
-- raw miss fraction denominator is on-time + missed. Missing coverage remains unknown.
-- Missed means dropped: not delivered within the on-time window, even if it arrived
-- later.
WITH per_event AS
(
    SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id, provider,
           min(event_utc) AS event_utc,
           countIf(outcome = 'on_time') > 0 AS delivered,
           countIf(outcome = 'missed') > 0 AS missed,
           minIf(assumeNotNull(lag_ns), outcome = 'on_time') AS lag_ns
    FROM benchmark_event_lags
    WHERE cloud_provider != '' AND event_utc >= {from_utc:DateTime64(9)} AND event_utc < {to_utc:DateTime64(9)}
      AND subscription_id = {subscription_id:String}
    GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id, provider
)
SELECT provider, data_type, commitment,
       countIf(delivered) AS on_time_events, countIf(missed) AS raw_missed_events,
       countIf(delivered AND lag_ns = 0) AS first_arrivals_including_ties,
       countIf(delivered AND lag_ns = 0) / nullIf(countIf(delivered OR missed), 0) AS first_arrival_share,
       countIf(missed) / nullIf(countIf(delivered OR missed), 0) AS uncertified_raw_miss_fraction,
       avgIf(lag_ns, delivered) AS avg_lag_ns,
       quantileExactIf(0.5)(lag_ns, delivered) AS p50_lag_ns,
       quantileExactIf(0.95)(lag_ns, delivered) AS p95_lag_ns
FROM per_event GROUP BY provider, data_type, commitment ORDER BY data_type, commitment, p50_lag_ns;

-- DROPS, CONNECTIVITY, HEALTH: report each separately. Unknown coverage
-- applies even if there are no benchmark_measurement_health rows (crash/sink failure might
-- never reach ClickHouse). These counts are not a certified completeness measure.
SELECT cloud_provider, region, subscription_id, provider, data_type, commitment,
       uniqExactIf(event_id, outcome = 'missed') AS missed_events
FROM benchmark_event_lags
WHERE cloud_provider != '' AND event_utc >= {from_utc:DateTime64(9)} AND event_utc < {to_utc:DateTime64(9)}
GROUP BY cloud_provider, region, subscription_id, provider, data_type, commitment
ORDER BY cloud_provider, provider, data_type, commitment;

SELECT cloud_provider, region, subscription_id, provider, data_type, commitment,
       event_utc, connected, reason
FROM benchmark_connection_events
WHERE cloud_provider != '' AND event_utc >= {from_utc:DateTime64(9)} AND event_utc < {to_utc:DateTime64(9)}
ORDER BY cloud_provider, provider, event_utc;

SELECT cloud_provider, region, subscription_id, provider, data_type, commitment,
       interval_start, interval_end, status, reason
FROM benchmark_measurement_health
WHERE cloud_provider != '' AND interval_start < {to_utc:DateTime64(9)}
  AND (interval_end IS NULL OR interval_end >= {from_utc:DateTime64(9)})
ORDER BY cloud_provider, interval_start;
-- Only status=healthy rows affirm coverage. Intervals with no healthy row are
-- UNKNOWN, not zero misses. Gap rows supersede overlapping healthy evidence.
-- Healthy intervals overlap by a comparison window; do not sum their durations.
-- To certify a particular miss, require its entire comparison window within a
-- healthy interval for that provider and exclude intersecting unknown gap rows.
-- The raw leaderboard above deliberately does NOT assert certified reliability.

-- BY REGION: same-subscriber arrivals only. Do not pair regional UTC clocks.
WITH per_event AS
(
    SELECT region, cloud_provider, subscription_id, data_type, commitment, provider, event_id,
           countIf(outcome = 'on_time') > 0 AS delivered,
           countIf(outcome = 'missed') > 0 AS missed,
           minIf(assumeNotNull(lag_ns), outcome = 'on_time') AS lag_ns
    FROM benchmark_event_lags
    WHERE cloud_provider != '' AND event_utc >= {from_utc:DateTime64(9)} AND event_utc < {to_utc:DateTime64(9)}
      AND subscription_id = {subscription_id:String}
    GROUP BY region, cloud_provider, subscription_id, data_type, commitment, provider, event_id
)
SELECT region, cloud_provider, provider, data_type, commitment,
       countIf(delivered) AS on_time_events, countIf(missed) AS missed_events,
       countIf(delivered AND lag_ns = 0) AS first_arrivals_including_ties,
       quantileExactIf(0.5)(lag_ns, delivered) AS p50_lag_ns,
       quantileExactIf(0.95)(lag_ns, delivered) AS p95_lag_ns
FROM per_event GROUP BY region, cloud_provider, provider, data_type, commitment
ORDER BY region, cloud_provider, data_type, commitment, p50_lag_ns;

-- SHORT-RANGE COVERAGE GATE: run separately for every provider/subscription and
-- short bucket (within one coverage row, about a minute; pending certified time is
-- written when health is lost). For longer ranges split into
-- buckets and require every bucket healthy. Empty tables return 'unknown'. Even a
-- single gap (provider-specific or global) invalidates the claimed range.
-- Pass provider plus the same region/cloud_provider/subscription/from/to as the report.
SELECT if(
    countIf(status = 'healthy' AND provider = {provider:String}
            AND interval_start <= {from_utc:DateTime64(9)}
            AND interval_end >= {to_utc:DateTime64(9)}) > 0
    AND countIf(status = 'unknown'
                AND (provider = {provider:String} OR provider IS NULL)
                AND interval_start >= {from_utc:DateTime64(9)}
                AND interval_start < {to_utc:DateTime64(9)}) = 0,
    'healthy', 'unknown') AS full_range_coverage
FROM benchmark_measurement_health
WHERE region = {region:String}
  AND cloud_provider != ''
  AND cloud_provider = {cloud_provider:String}
  AND subscription_id = {subscription_id:String}
  AND interval_start < {to_utc:DateTime64(9)};
-- Raw missed counts in the leaderboard are never proof that every event on
-- chain was observed; the benchmark has no independent completeness oracle.
