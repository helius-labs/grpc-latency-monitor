-- One minute of raw rows -> benchmark_minute_rollups rows. Shared by the subscriber's
-- rollup worker (src/rollups.rs), the dashboards' raw fallback (grafana/rollups.ts)
-- and the SQL tests, so summaries and exact queries can never diverge.
--
-- Placeholders in double braces are replaced as text before use:
--   source_filter  WHERE condition selecting the raw rows to summarize
--   bucket         minute bucket (the worker passes one minute; dashboards derive it per row)
--   generation     summary generation (0 for the dashboards' raw fallback)
--
-- Competitors of an event are providers that delivered it on time or dropped it
-- (`missed`) while connected. A `forked` miss (its slot never confirmed) is neither.
-- A provider win needs at least two competitors and one unique first on-time
-- delivery, so a provider that drops an event loses to every provider that
-- delivered it. Pair rows cover both orientations whenever both providers competed
-- and at least one delivered: differences (`delivered`, lag sum, percentiles) use
-- only events both delivered, while delivering an event the other provider dropped
-- counts as a win.
WITH per_provider AS (
 SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id, provider,
 min(event_utc) first_seen, max(event_utc) last_seen,
 toUInt8(countIf(outcome = 'on_time') > 0) delivered,
 toUInt8(countIf(outcome = 'missed') > 0) missed,
 toInt64(minIf(assumeNotNull(lag_ns), outcome = 'on_time')) lag,
 maxIf(message_bytes, outcome = 'on_time') bytes
 FROM benchmark_event_lags
 WHERE {{source_filter}}
 AND outcome IN ('on_time', 'missed')
 GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id, provider
), events AS (
 SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id,
 groupArray(tuple(provider, first_seen, last_seen, delivered, missed, lag, bytes)) providers
 FROM per_provider GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id
), expanded AS (
 SELECT region, cloud_provider, subscription_id, data_type, commitment,
 arrayCount(p -> p.4 = 1 OR p.5 = 1, providers) >= 2
   AND arrayCount(p -> p.4 = 1 AND p.6 = 0, providers) = 1 eligible,
 arrayJoin(arrayConcat(
   arrayMap(p -> tuple('provider', p.1, '', p.2, p.3, p.4, p.5, p.6,
     toUInt8(eligible AND p.4 = 1 AND p.6 = 0), toUInt8(0),
     toUInt8(p.4 = 1 AND data_type IN ('account', 'transaction')),
     toUInt8(p.4 = 1 AND data_type IN ('account', 'transaction') AND isNull(p.7)),
     if(p.4 = 1 AND data_type IN ('account', 'transaction'), ifNull(p.7, 0), toUInt64(0))), providers),
   arrayFlatten(arrayMap(a -> arrayMap(b ->
     tuple('pair', a.1, b.1, a.2, a.3, toUInt8(a.4 = 1 AND b.4 = 1), toUInt8(0),
       if(a.4 = 1 AND b.4 = 1, a.6 - b.6, 0),
       toUInt8(a.4 = 1 AND (b.4 = 0 OR a.6 < b.6)), toUInt8(b.4 = 1 AND (a.4 = 0 OR a.6 > b.6)),
       toUInt8(0), toUInt8(0), toUInt64(0)),
     arrayFilter(b -> b.1 != a.1 AND (b.4 = 1 OR b.5 = 1) AND (a.4 = 1 OR b.4 = 1), providers)),
     arrayFilter(a -> a.4 = 1 OR a.5 = 1, providers)))
 )) row
 FROM events
)
SELECT {{bucket}} AS bucket, {{generation}} AS generation, region, cloud_provider, subscription_id, data_type, commitment,
 row.1 kind, row.2 provider, row.3 other_provider,
 min(row.4) AS first_seen, max(row.5) AS last_seen, count() AS observations, sum(row.6) AS delivered, sum(row.7) AS missed, sum(row.9) AS wins, sum(row.10) AS losses,
 sum(toInt128(if(row.6 = 1, row.8, 0))) AS lag_sum, sum(row.11) AS byte_events, sum(row.12) AS unknown_bytes, sum(row.13) AS message_bytes,
 quantilesTDigestIfState(0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999)(row.8, row.6) AS percentiles
FROM expanded
GROUP BY bucket, region, cloud_provider, subscription_id, data_type, commitment, kind, provider, other_provider
