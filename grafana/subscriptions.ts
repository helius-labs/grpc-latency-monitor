import { activity, withRollups } from './rollups.js';

// Keep configuration selection independent of provider selection so every panel
// compares the same subscriptions. Resolve automatic mode on every refresh.
export const dataTypeScope = `(\${data_type:sqlstring} = '__all' OR data_type = \${data_type:sqlstring})`;
export const streamScope = `region = \${region:sqlstring} AND cloud_provider = \${cloud_provider:sqlstring}
 AND ${dataTypeScope} AND commitment = \${commitment:sqlstring}`;

// Connection history covers an outage even when no event was delivered in range.
// Match the downtime query's lookback, and never select a future configuration.
export function connectionActivity(filter: string): string {
  return `SELECT region, cloud_provider, data_type, subscription_id, provider, event_utc AS last_seen
 FROM benchmark_connection_events WHERE ${filter}
 AND event_utc >= $__fromTime_ms - INTERVAL 1 DAY AND event_utc <= $__toTime_ms`;
}

// Prefer observed events, keeping the measurement configuration unchanged. Only
// a stream without observations falls back to its latest connection history.
export function latestWithConnections(observed: string, filter: string, keys = 'data_type'): string {
  return `SELECT ${keys},
 argMax(subscription_id, tuple(observed, last_seen, subscription_id)) AS subscription_id
 FROM (SELECT ${keys}, subscription_id, last_seen, toUInt8(1) AS observed FROM (${observed})
 UNION ALL SELECT ${keys}, subscription_id, last_seen, toUInt8(0) AS observed
 FROM (${connectionActivity(filter)})) GROUP BY ${keys}`;
}

// Observations come from committed summaries plus raw rows for unsummarized minutes.
const observed = activity(streamScope);
const latestSubscriptions = latestWithConnections(observed, streamScope);

export const subscriptionScope = `${streamScope}
 AND (
   ((\${data_type:sqlstring} = '__all' OR \${subscription_id:sqlstring} = '__latest')
     AND (data_type, subscription_id) IN (${latestSubscriptions}))
   OR (\${data_type:sqlstring} != '__all' AND \${subscription_id:sqlstring} != '__latest'
     AND subscription_id = \${subscription_id:sqlstring})
 )`;

const configurations = `WITH observed AS (
 SELECT subscription_id, data_type, min(first_seen) AS first_seen, max(last_seen) AS last_seen
 FROM (${observed}) GROUP BY subscription_id, data_type
), metadata AS (
 SELECT subscription_id, argMax(filters, registered_utc) AS filters
 FROM benchmark_subscription_versions
 WHERE ${dataTypeScope} AND commitment = \${commitment:sqlstring}
 GROUP BY subscription_id
), configurations AS (
 SELECT observed.*, metadata.filters AS filters,
 extractAll(metadata.filters, '"([^"]+)"') AS addresses,
 extract(metadata.filters, ';window=([^;]+)') AS window_setting,
 replaceAll(replaceRegexpAll(extract(metadata.filters, ';providers=([^;]+)'), '@[0-9a-f]+', ''), ',', ', ') AS provider_names,
 concat(multiIf(empty(metadata.filters), concat('Configuration ', substring(observed.subscription_id, 1, 8)),
   observed.data_type = 'slot', 'Slot updates',
   concat(if(observed.data_type = 'account', 'Account owner: ', 'Transactions include: '),
     if(empty(addresses), 'all', arrayStringConcat(arrayMap(address ->
       concat(substring(address, 1, 6), '…', substring(address, -4)), arraySlice(addresses, 1, 3)), ', ')),
     if(length(addresses) > 3, concat(' +', toString(length(addresses) - 3), ' more'), ''))),
   if(window_setting = '', '', concat(' · window ', window_setting)),
   if(provider_names = '', '', concat(' · ', provider_names)))
 AS configuration
 FROM observed LEFT JOIN metadata ON observed.subscription_id = metadata.subscription_id
)`;

// A historical ID selects one stream. In All mode only the static automatic
// option is offered, and stale bookmarked IDs are ignored by subscriptionScope.
export const subscriptionOptions = withRollups(configurations + `
 SELECT subscription_id AS value,
 concat(configuration, ' · ', formatDateTime(last_seen, '%Y-%m-%d %H:%i:%S'), ' UTC') AS label
 FROM configurations WHERE \${data_type:sqlstring} != '__all'
 ORDER BY last_seen DESC, subscription_id DESC`);

export const subscriptionDetails = withRollups(configurations + `
 SELECT data_type AS stream, configuration, first_seen, last_seen, filters, subscription_id
 FROM configurations ORDER BY data_type, last_seen DESC, subscription_id DESC`);

// Providers with observations or connection history for the selected streams.
export const providerOptions = withRollups(`SELECT DISTINCT provider FROM (
 SELECT provider FROM (${observed})
 UNION ALL SELECT provider FROM (${connectionActivity(streamScope)})
) ORDER BY provider`);
