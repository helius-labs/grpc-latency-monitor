// ClickHouse SQL for the All Providers and Head to Head dashboards.
//
// Panels that can span long ranges have two forms: an exact one over raw rows
// (retained six hours) and one over committed minute summaries. `route` picks one
// on every refresh from the time range and bucket size; `exactQueries` exposes the
// exact forms so the SQL tests can check that both agree.
import { dataTypeScope, subscriptionScope as scope } from './subscriptions.js';
import { activity, completeRawMinutes, states, withRollups } from './rollups.js';

const displayProviders = '$__conditionalAll(provider IN (${provider:sqlstring}), $provider)';
const pairProviders = 'provider IN (${provider_a:sqlstring}, ${provider_b:sqlstring})';

// One row per provider and event; retries are deduplicated. `providers` limits the
// rows to the displayed providers (null keeps every provider).
function perEvent(filter: string, providers: string | null = displayProviders): string {
  return `WITH per_event AS (
 SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id, provider,
 min(event_utc) AS observed_utc,
 countIf(outcome = 'on_time') > 0 AS delivered,
 countIf(outcome = 'missed') > 0 AS missed,
 maxIf(message_bytes, outcome = 'on_time') AS message_bytes,
 minIf(assumeNotNull(lag_ns), outcome = 'on_time') AS lag_ns
 FROM benchmark_event_lags WHERE ${filter}
AND $__timeFilter_ms(event_utc)
AND ${completeRawMinutes}
AND outcome IN ('on_time', 'missed')
${providers === null ? '' : `AND ${providers}`}
 GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id, provider
)
`;
}

// Determine outright winners before applying the display filter. A hidden
// provider remains part of the ideal baseline and can prevent a displayed win.
// Competitors delivered on time or dropped the event while connected. A win needs
// two competitors and a unique first on-time delivery: dropping an event loses it
// to every provider that delivered it. Uncontested deliveries and ties don't count.
const providerWins = `WITH event_winners AS (
 SELECT subscription_id, data_type, commitment, event_id, min(event_utc) AS winner_utc,
 uniqExactIf(provider, outcome IN ('on_time', 'missed')) >= 2
   AND uniqExactIf(provider, outcome = 'on_time' AND lag_ns = 0) = 1 AS eligible,
 minIf(provider, outcome = 'on_time' AND lag_ns = 0) AS winner,
 groupUniqArrayIf(provider, outcome IN ('on_time', 'missed')) AS providers
 FROM benchmark_event_lags WHERE ${scope} AND $__timeFilter_ms(event_utc)
 AND ${completeRawMinutes}
 GROUP BY subscription_id, data_type, commitment, event_id
), selected_events AS (
 SELECT provider, winner_utc AS event_utc, eligible AND provider = winner AS is_win
 FROM event_winners ARRAY JOIN providers AS provider
 WHERE $__conditionalAll(provider IN (\${provider:sqlstring}), $provider)
)
`;

// Conditional aggregates pair providers in one scan and preserve retry deduplication.
// Every event both providers competed for (on time, or dropped while connected)
// that at least one delivered. Differences use only events both delivered on time;
// delivering an event the other provider dropped is a win.
const pairs = `WITH pairs AS (
 SELECT subscription_id, data_type, commitment, event_id,
 minIf(event_utc, provider = \${provider_a:sqlstring}) AS observed_utc,
 countIf(provider = \${provider_a:sqlstring} AND outcome = 'on_time') > 0 AS a_on_time,
 countIf(provider = \${provider_b:sqlstring} AND outcome = 'on_time') > 0 AS b_on_time,
 a_on_time AND b_on_time AS both_on_time,
 maxIf(message_bytes, provider = \${provider_a:sqlstring} AND outcome = 'on_time') AS a_bytes,
 maxIf(message_bytes, provider = \${provider_b:sqlstring} AND outcome = 'on_time') AS b_bytes,
 if(both_on_time, toInt64(minIf(assumeNotNull(lag_ns), provider = \${provider_a:sqlstring} AND outcome = 'on_time'))
   - toInt64(minIf(assumeNotNull(lag_ns), provider = \${provider_b:sqlstring} AND outcome = 'on_time')), 0) AS difference_ns,
 (both_on_time AND difference_ns < 0) OR (a_on_time AND NOT b_on_time) AS a_win,
 (both_on_time AND difference_ns > 0) OR (b_on_time AND NOT a_on_time) AS b_win
 FROM benchmark_event_lags WHERE ${scope} AND $__timeFilter_ms(event_utc)
 AND ${completeRawMinutes}
 AND outcome IN ('on_time', 'missed') AND provider IN (\${provider_a:sqlstring}, \${provider_b:sqlstring})
 GROUP BY subscription_id, data_type, commitment, event_id
 HAVING countIf(provider = \${provider_a:sqlstring}) > 0
   AND countIf(provider = \${provider_b:sqlstring}) > 0
   AND (a_on_time OR b_on_time)
   AND \${provider_a:sqlstring} != \${provider_b:sqlstring}
)
`;

// Exact forms over raw rows. Shared dashboard results: one backend query per group
// of related panels.
const exactPairSummary = pairs + `SELECT * EXCEPT percentiles FROM (SELECT
 quantilesExactIf(0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999)(difference_ns, both_on_time) AS percentiles,
 if(countIf(both_on_time) > 0, percentiles[1] / 1e6, NULL) AS p001_ms,
 if(countIf(both_on_time) > 0, percentiles[2] / 1e6, NULL) AS p1_ms,
 if(countIf(both_on_time) > 0, percentiles[3] / 1e6, NULL) AS p5_ms,
 if(countIf(both_on_time) > 0, percentiles[4] / 1e6, NULL) AS p50_ms,
 if(countIf(both_on_time) > 0, percentiles[5] / 1e6, NULL) AS p95_ms,
 if(countIf(both_on_time) > 0, percentiles[6] / 1e6, NULL) AS p99_ms,
 if(countIf(both_on_time) > 0, percentiles[7] / 1e6, NULL) AS p999_ms,
 if(countIf(both_on_time) > 0, avgIf(difference_ns, both_on_time) / 1e6, NULL) AS avg_ms,
 countIf(a_win) / nullIf(countIf(a_win OR b_win), 0) * 100 AS a_win_percent,
 countIf(b_win) / nullIf(countIf(a_win OR b_win), 0) * 100 AS b_win_percent
 FROM pairs)`;

// Buckets with only dropped-event wins have no differences: those fields are NULL.
const exactPairTrends = pairs + `SELECT * EXCEPT percentiles FROM (SELECT $__timeInterval(observed_utc) AS time,
 quantilesExactIf(0.01, 0.05, 0.5, 0.95, 0.99)(difference_ns, both_on_time) AS percentiles,
 if(countIf(both_on_time) > 0, avgIf(difference_ns, both_on_time) / 1e6, NULL) AS avg_a_minus_b_ms,
 if(countIf(both_on_time) > 0, percentiles[1] / 1e6, NULL) AS p1_a_minus_b_ms,
 if(countIf(both_on_time) > 0, percentiles[2] / 1e6, NULL) AS p5_a_minus_b_ms,
 if(countIf(both_on_time) > 0, percentiles[3] / 1e6, NULL) AS median_a_minus_b_ms,
 if(countIf(both_on_time) > 0, percentiles[4] / 1e6, NULL) AS p95_a_minus_b_ms,
 if(countIf(both_on_time) > 0, percentiles[5] / 1e6, NULL) AS p99_a_minus_b_ms,
 countIf(a_win) / nullIf(countIf(a_win OR b_win), 0) * 100 AS a_win_percent,
 countIf(b_win) / nullIf(countIf(a_win OR b_win), 0) * 100 AS b_win_percent
 FROM pairs GROUP BY time ORDER BY time)`;

// Percentiles summarize each provider's own on-time deliveries against the stored
// first-arrival baseline, which includes every configured provider.
const exactProviderLagSummaries = perEvent(scope) + `SELECT * EXCEPT percentiles FROM (SELECT provider,
 quantilesExactIf(0.5, 0.99)(lag_ns, delivered) AS percentiles,
 if(countIf(delivered) > 0, percentiles[1] / 1e6, NULL) AS p50_lag_ms,
 if(countIf(delivered) > 0, percentiles[2] / 1e6, NULL) AS p99_lag_ms
 FROM per_event GROUP BY provider ORDER BY provider)`;

// Throughput matches the event-rate population exactly; unknown sizes must not
// become zero throughput.
const exactProviderMetricsTrend = perEvent(scope) + `SELECT * EXCEPT percentiles FROM (SELECT $__timeInterval(observed_utc) AS time, provider,
 quantilesExactIf(0.5, 0.95, 0.99, 0.999)(lag_ns, delivered) AS percentiles,
 if(countIf(delivered) > 0, percentiles[1] / 1e6, NULL) AS p50_lag_ms,
 if(countIf(delivered) > 0, percentiles[2] / 1e6, NULL) AS p95_lag_ms,
 if(countIf(delivered) > 0, percentiles[3] / 1e6, NULL) AS p99_lag_ms,
 if(countIf(delivered) > 0, percentiles[4] / 1e6, NULL) AS p999_lag_ms,
 countIf(delivered) / $__interval_s AS events_per_second,
 countIf(missed) / $__interval_s AS dropped_per_second,
 if(countIf(delivered AND data_type IN ('account', 'transaction')) > 0
   AND countIf(delivered AND data_type IN ('account', 'transaction') AND isNotNull(message_bytes))
     = countIf(delivered AND data_type IN ('account', 'transaction')),
   sumIf(message_bytes, delivered AND data_type IN ('account', 'transaction')) / $__interval_s, NULL) AS bytes_per_second
 FROM per_event GROUP BY time, provider ORDER BY time, provider)`;

const exactProviderWinSummary = providerWins + `SELECT provider,
 countIf(is_win) / nullIf(sum(countIf(is_win)) OVER (), 0) * 100 AS win_percent
 FROM selected_events GROUP BY provider ORDER BY win_percent DESC NULLS LAST, provider`;

const exactProviderWinTrend = providerWins + `SELECT $__timeInterval(event_utc) AS time, provider,
 countIf(is_win) / nullIf(sum(countIf(is_win)) OVER (PARTITION BY time), 0) * 100 AS win_percent
 FROM selected_events GROUP BY time, provider ORDER BY time, provider`;

// Dropped = a connected provider did not deliver within the on-time window, even
// if its delivery arrived later. One series per provider; retries are deduplicated.
const exactPairDroppedRate = perEvent(scope, pairProviders) + `SELECT $__timeInterval(observed_utc) AS time,
 provider AS series, countIf(missed) / $__interval_s AS events_per_second
 FROM per_event GROUP BY time, series ORDER BY time`;

// The map picks the most recently observed configuration within the time range
// separately for every subscriber: configuration IDs can differ across locations.
const mapFilter = `${dataTypeScope} AND commitment = \${commitment:sqlstring}`;
const latestMap = `SELECT region, cloud_provider, data_type,
 argMax(subscription_id, tuple(last_seen, subscription_id)) FROM (${activity(mapFilter)})
 GROUP BY region, cloud_provider, data_type`;
const mapScope = `${mapFilter} AND (region, cloud_provider, data_type, subscription_id) IN (${latestMap})`;
// Per-location provider wins and lags -> one marker per location.
const mapMarkers = `), summaries AS (
 SELECT region, cloud_provider, sum(wins) AS total_wins,
 max(wins) AS most_wins, arraySort(groupArray(tuple(provider, wins, p50_ms, p95_ms))) AS providers
 FROM wins GROUP BY region, cloud_provider
), locations AS (
 SELECT region, cloud_provider,
 argMax(tuple(latitude, longitude), registered_utc) AS coordinates
 FROM benchmark_subscriber_locations GROUP BY region, cloud_provider
), markers AS (
 SELECT summaries.region AS region, summaries.cloud_provider AS cloud_provider,
 concat(summaries.region, ' · ', summaries.cloud_provider) AS location,
 locations.coordinates.1 AS latitude,
 locations.coordinates.2 AS longitude,
 summaries.total_wins AS total_wins,
 arrayStringConcat(arrayMap(p -> concat(p.1, ': ',
   if(summaries.total_wins = 0, 'No wins', concat(toString(round(p.2 / summaries.total_wins * 100, 2)), '%'))),
   summaries.providers), ' · ') AS win_rates,
 arrayStringConcat(arrayMap(p -> concat(p.1, ': ',
   if(isNull(p.3), 'No data', concat(toString(round(p.3, 2)), ' ms'))), summaries.providers), ' · ') AS provider_p50,
 arrayStringConcat(arrayMap(p -> concat(p.1, ': ',
   if(isNull(p.4), 'No data', concat(toString(round(p.4, 2)), ' ms'))), summaries.providers), ' · ') AS provider_p95,
 multiIf(summaries.total_wins = 0, 'No wins',
   arrayCount(p -> p.2 = summaries.most_wins, summaries.providers) > 1, 'Tie',
   arrayFirst(p -> p.2 = summaries.most_wins, summaries.providers).1) AS leader
 FROM summaries INNER JOIN locations
 ON summaries.region = locations.region AND summaries.cloud_provider = locations.cloud_provider
)
SELECT *,
 if(leader IN ('No wins', 'Tie'), 0, 1 + cityHash64(leader) % 8) AS leader_color,
 greatest(-180., least(180., longitude + 0.02 * (
   toFloat64(row_number() OVER (PARTITION BY latitude, longitude ORDER BY region, cloud_provider))
   - (count() OVER (PARTITION BY latitude, longitude) + 1) / 2.0))) AS marker_longitude
FROM markers ORDER BY region, cloud_provider`;

// One event pipeline: collect providers once, choose the winner, then expand.
// Avoid referencing per_event through two branches and joining millions of rows.
const exactLocationWinRates = perEvent(mapScope, null).trimEnd() + `, event_groups AS (
 SELECT region, cloud_provider, subscription_id, data_type, commitment, event_id,
 groupArray(tuple(provider, delivered, lag_ns, missed)) AS providers
 FROM per_event GROUP BY region, cloud_provider, subscription_id, data_type, commitment, event_id
), expanded AS (
 SELECT region, cloud_provider,
 arrayCount(p -> p.2 OR p.4, providers) >= 2 AND arrayCount(p -> p.2 AND p.3 = 0, providers) = 1 AS eligible,
 arrayJoin(providers) AS observation
 FROM event_groups
), wins AS (
 SELECT region, cloud_provider, observation.1 AS provider,
 countIf(eligible AND observation.2 AND observation.3 = 0) AS wins,
 if(countIf(observation.2) > 0, quantileExactIf(0.5)(observation.3, observation.2) / 1e6, NULL) AS p50_ms,
 if(countIf(observation.2) > 0, quantileExactIf(0.95)(observation.3, observation.2) / 1e6, NULL) AS p95_ms
 FROM expanded GROUP BY region, cloud_provider, provider
${mapMarkers}`;

function connectionEvents(providers: string, connected: boolean): string {
  return `SELECT event_utc AS time,
 concat(if(\${data_type:sqlstring} = '__all', concat(provider, ' / ', data_type), provider), ': ', if(connected, 'connected', 'disconnected'), ' — ', reason) AS text,
 concat(provider, ',', data_type, ',', if(connected, 'connected', 'disconnected')) AS tags
 FROM benchmark_connection_events WHERE ${scope}
 AND $__timeFilter_ms(event_utc)
 AND ${providers}
 AND connected = ${connected} ORDER BY event_utc`;
}

// A disconnected period shorter than this is a reconnect, not downtime: a rolling
// restart behind one endpoint drops the stream, but resubscribing works right away.
export const OUTAGE_SECONDS = 10;

// Downtime per `keys`: outages where reconnecting kept failing. An outage runs from
// a disconnect, through failed reconnect attempts, to the next successful
// subscription. Outages lasting at least OUTAGE_SECONDS count in full, clipped to
// the range and summed over streams; shorter ones count only as disconnects. An
// outage still open at the range end is judged by how long it lasted afterwards
// (or until now). Events while a provider is disconnected are excluded from
// comparisons instead of counted as dropped, so downtime is reported here.
// A subscriber restart without a recorded disconnect is not provider downtime, and
// neither is a disconnect caused by the subscriber's own backlog (reason texts from
// LOCAL_DISCONNECTS in src/stream.rs); a provider failure while reconnecting is.
export const localDisconnects = `('arrival queue full', 'control queue full')`;
export function downtime(keys: string, where: string): string {
  return `SELECT ${keys},
 sumIf(greatest(0, dateDiff('millisecond', greatest(start_utc, range_start), least(end_utc, range_end))),
   dateDiff('millisecond', start_utc, end_utc) >= ${OUTAGE_SECONDS * 1000}) / 1000 AS disconnected_seconds,
 countIf(start_utc >= range_start AND start_utc <= range_end) AS disconnects
 FROM (SELECT ${keys}, min(event_utc) AS start_utc, max(next_utc) AS end_utc,
  any(range_start) AS range_start, any(range_end) AS range_end
  FROM (SELECT ${keys}, subscription_id, event_utc,
   NOT connected AND reason NOT IN ${localDisconnects} AS provider_down,
   CAST($__fromTime_ms AS DateTime64(9, 'UTC')) AS range_start,
   CAST($__toTime_ms AS DateTime64(9, 'UTC')) AS range_end,
   -- Rows of one outage share the last transition before them that was not downtime.
   maxIf(event_utc, connected OR reason IN ${localDisconnects}) OVER before AS outage,
   leadInFrame(event_utc, 1, least(now64(9, 'UTC'), range_end + INTERVAL ${OUTAGE_SECONDS} SECOND)) OVER stream AS next_utc
   FROM benchmark_connection_events WHERE ${where}
   AND event_utc <= $__toTime_ms + INTERVAL ${OUTAGE_SECONDS} SECOND AND event_utc >= $__fromTime_ms - INTERVAL 1 DAY
   WINDOW stream AS (PARTITION BY ${keys}, subscription_id ORDER BY event_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
   before AS (PARTITION BY ${keys}, subscription_id ORDER BY event_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING))
  WHERE provider_down GROUP BY ${keys}, subscription_id, outage)
 GROUP BY ${keys}`;
}

function providerDowntimeRows(providers: string): string {
  return downtime('provider', `${scope}
   AND ${providers}`) + ' ORDER BY provider';
}

// Summary forms over committed minutes (plus raw rows for unsummarized minutes).
const levels = '0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999';
const merged = `quantilesTDigestIfMerge(${levels})(percentiles)`;
// One-minute tolerance avoids switching a live 6h range because of request latency.
const historical = '$__fromTime_ms < now() - INTERVAL 6 HOUR - INTERVAL 1 MINUTE';
const longDetail = `(dateDiff('second', $__fromTime_ms, $__toTime_ms) > 21600 OR ${historical})`;
// Range totals use summaries beyond one hour; trends keep ten-second detail for six.
const overview = `(dateDiff('second', $__fromTime_ms, $__toTime_ms) > 3600 OR ${historical})`;
const summaryInterval = `if(${overview}, greatest(60, intDiv($__interval_s + 59, 60) * 60), $__interval_s)`;
const coarse = `(($__interval_s >= 60 AND modulo($__interval_s, 60) = 0) OR ${longDetail})`;
const pair = "kind = 'pair' AND provider = ${provider_a:sqlstring} AND other_provider = ${provider_b:sqlstring}";
function route(exact: string, summary: string, condition: string): string {
  summary = summary.replaceAll('$__interval_s', summaryInterval)
    .replaceAll('$__timeInterval(bucket)', `toStartOfInterval(bucket, INTERVAL ${summaryInterval} SECOND)`);
  return withRollups(`SELECT * FROM (${exact}) WHERE NOT (${condition})
 UNION ALL SELECT * FROM (${summary}) WHERE ${condition}`) + `
 SETTINGS max_threads = 16, max_memory_usage = 6000000000, max_execution_time = 45,
 max_bytes_before_external_group_by = 2000000000, max_bytes_before_external_sort = 1000000000`;
}
function source(filter = scope): string { return `(${states(filter)})`; }
const providerRows = `FROM ${source()} WHERE kind = 'provider' AND ${displayProviders}`;
const pairRows = `FROM ${source()} WHERE ${pair}`;
const summary = (body: string) => `SELECT * EXCEPT qs FROM (${body})`;

export const pairSummary = route(exactPairSummary, summary(`SELECT ${merged} qs,
 if(sum(delivered) > 0, qs[1] / 1e6, NULL) p001_ms,
 if(sum(delivered) > 0, qs[2] / 1e6, NULL) p1_ms,
 if(sum(delivered) > 0, qs[3] / 1e6, NULL) p5_ms,
 if(sum(delivered) > 0, qs[4] / 1e6, NULL) p50_ms,
 if(sum(delivered) > 0, qs[5] / 1e6, NULL) p95_ms,
 if(sum(delivered) > 0, qs[6] / 1e6, NULL) p99_ms,
 if(sum(delivered) > 0, qs[7] / 1e6, NULL) p999_ms,
 toFloat64(sum(lag_sum)) / nullIf(sum(delivered), 0) / 1e6 avg_ms,
 sum(wins) / nullIf(sum(wins) + sum(losses), 0) * 100 a_win_percent,
 sum(losses) / nullIf(sum(wins) + sum(losses), 0) * 100 b_win_percent ${pairRows}`), overview);

// Pair rows also carry dropped-event wins (delivered = 0): no differences there.
export const pairTrends = route(exactPairTrends, summary(`SELECT $__timeInterval(bucket) time, ${merged} qs,
 toFloat64(sum(lag_sum)) / nullIf(sum(delivered), 0) / 1e6 avg_a_minus_b_ms,
 if(sum(delivered) > 0, qs[2] / 1e6, NULL) p1_a_minus_b_ms, if(sum(delivered) > 0, qs[3] / 1e6, NULL) p5_a_minus_b_ms,
 if(sum(delivered) > 0, qs[4] / 1e6, NULL) median_a_minus_b_ms,
 if(sum(delivered) > 0, qs[5] / 1e6, NULL) p95_a_minus_b_ms, if(sum(delivered) > 0, qs[6] / 1e6, NULL) p99_a_minus_b_ms,
 sum(wins) / nullIf(sum(wins) + sum(losses), 0) * 100 a_win_percent,
 sum(losses) / nullIf(sum(wins) + sum(losses), 0) * 100 b_win_percent
 ${pairRows} GROUP BY time ORDER BY time`), coarse);

export const providerLagSummaries = route(exactProviderLagSummaries, summary(`SELECT provider, ${merged} qs,
 if(sum(delivered) > 0, qs[4] / 1e6, NULL) p50_lag_ms,
 if(sum(delivered) > 0, qs[6] / 1e6, NULL) p99_lag_ms
 ${providerRows} GROUP BY provider ORDER BY provider`), overview);

export const providerMetricsTrend = route(exactProviderMetricsTrend, summary(`SELECT $__timeInterval(bucket) time, provider, ${merged} qs,
 if(sum(delivered) > 0, qs[4] / 1e6, NULL) p50_lag_ms,
 if(sum(delivered) > 0, qs[5] / 1e6, NULL) p95_lag_ms,
 if(sum(delivered) > 0, qs[6] / 1e6, NULL) p99_lag_ms,
 if(sum(delivered) > 0, qs[7] / 1e6, NULL) p999_lag_ms,
 sum(delivered) / $__interval_s events_per_second, sum(missed) / $__interval_s dropped_per_second,
 if(sum(byte_events) > 0 AND sum(unknown_bytes) = 0, sum(message_bytes) / $__interval_s, NULL) bytes_per_second
 ${providerRows} GROUP BY time, provider ORDER BY time, provider`), coarse);

export const providerWinSummary = route(exactProviderWinSummary, `SELECT provider,
 sum(wins) / nullIf(sum(sum(wins)) OVER (), 0) * 100 win_percent
 ${providerRows} GROUP BY provider ORDER BY win_percent DESC NULLS LAST, provider`, overview);
export const providerWinTrend = route(exactProviderWinTrend, `SELECT $__timeInterval(bucket) time, provider,
 sum(wins) / nullIf(sum(sum(wins)) OVER (PARTITION BY time), 0) * 100 win_percent
 ${providerRows} GROUP BY time, provider ORDER BY time, provider`, coarse);
export const pairDroppedRate = route(exactPairDroppedRate, `SELECT $__timeInterval(bucket) time,
 provider series, sum(missed) / $__interval_s events_per_second
 FROM ${source()} WHERE kind = 'provider' AND ${pairProviders}
 GROUP BY time, series ORDER BY time`, coarse);

export const locationWinRates = route(exactLocationWinRates, `WITH wins AS (SELECT region, cloud_provider, provider, sum(wins) wins,
 if(sum(delivered) > 0, ${merged}[4] / 1e6, NULL) p50_ms,
 if(sum(delivered) > 0, ${merged}[5] / 1e6, NULL) p95_ms
 FROM ${source(mapScope)} WHERE kind = 'provider' GROUP BY region, cloud_provider, provider
 ${mapMarkers}`, overview);

export const connectedEvents = withRollups(connectionEvents(displayProviders, true));
export const disconnectedEvents = withRollups(connectionEvents(displayProviders, false));
export const pairConnectedEvents = withRollups(connectionEvents(pairProviders, true));
export const pairDisconnectedEvents = withRollups(connectionEvents(pairProviders, false));

// Providers with events but no recorded disconnects get zero, so every selected
// provider is listed by name (a lone bar gauge value hides its name). A provider
// down for the whole range has no events and comes from the downtime rows.
export const providerDowntime = withRollups(`SELECT provider,
 sum(disconnected_seconds) AS disconnected_seconds, sum(disconnects) AS disconnects
 FROM (SELECT DISTINCT provider, toFloat64(0) AS disconnected_seconds, toUInt64(0) AS disconnects
  FROM (${activity(scope)}) WHERE ${displayProviders}
  UNION ALL SELECT provider, disconnected_seconds, disconnects FROM (${providerDowntimeRows(displayProviders)}))
 GROUP BY provider ORDER BY provider`);
export const pairDowntime = withRollups(`SELECT
 sumIf(disconnected_seconds, provider = \${provider_a:sqlstring}) AS a_disconnected_seconds,
 sumIf(disconnected_seconds, provider = \${provider_b:sqlstring}) AS b_disconnected_seconds,
 sumIf(disconnects, provider = \${provider_a:sqlstring}) AS a_disconnects,
 sumIf(disconnects, provider = \${provider_b:sqlstring}) AS b_disconnects
 FROM (${providerDowntimeRows(pairProviders)})`);

export const exactQueries = Object.fromEntries(Object.entries({
  pairSummary: exactPairSummary,
  pairTrends: exactPairTrends,
  providerLagSummaries: exactProviderLagSummaries,
  providerMetricsTrend: exactProviderMetricsTrend,
  providerWinSummary: exactProviderWinSummary,
  providerWinTrend: exactProviderWinTrend,
  pairDroppedRate: exactPairDroppedRate,
  locationWinRates: exactLocationWinRates,
}).map(([name, sql]) => [name, withRollups(sql)]));
