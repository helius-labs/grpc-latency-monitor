import type { Panel } from '@grafana/grafana-foundation-sdk/dashboard';
import {
  FieldColorBuilder, FieldColorModeId, ThresholdsConfigBuilder, ThresholdsMode, MappingType, SpecialValueMatch, RowBuilder,
} from '@grafana/grafana-foundation-sdk/dashboard';
import { PanelBuilder as Table } from '@grafana/grafana-foundation-sdk/table';
import { TableCellDisplayMode, TableCellBackgroundDisplayMode } from '@grafana/grafana-foundation-sdk/common';
import { baseDashboard, addStreamVariables, selectFields, reuseResults } from './shared.js';
import { ClickHouseQueryBuilder, datasource } from './clickhouse.js';
import { dataTypeScope, commitmentScope, latestWithConnections } from './subscriptions.js';
import { committedMinutes, withRollups } from './rollups.js';
import { downtime, OUTAGE_SECONDS } from './queries.js';

// Committed minute summaries only: no raw fallback, so the newest partial
// minutes are not shown yet, but every query reads a few thousand rows at most.
export const overviewDashboard = baseDashboard('Solana Benchmark — Overview', 'solana-streaming-overview')
  .time({ from: 'now-1h', to: 'now' }).refresh('1m');
addStreamVariables(overviewDashboard);

const filter = `kind = 'provider' AND ${dataTypeScope} AND ${commitmentScope}
 AND bucket + INTERVAL 1 MINUTE > $__fromTime_ms AND bucket < $__toTime_ms
 AND (region, cloud_provider, bucket, generation) IN (${committedMinutes})`;
// Like the other dashboards, compare only each location's latest configuration per stream.
const latestStreams = `(region, cloud_provider, data_type, subscription_id) IN (
   ${latestWithConnections(`SELECT region, cloud_provider, data_type, subscription_id, last_seen
   FROM benchmark_minute_rollups WHERE ${filter}`, `${dataTypeScope} AND ${commitmentScope}`,
   'region, cloud_provider, data_type')})`;
const rollups = `SELECT * FROM benchmark_minute_rollups WHERE ${filter} AND ${latestStreams}`;
// Downtime per location and provider, measured like the All Providers dashboard:
// only outages of at least OUTAGE_SECONDS, from the disconnect to the next
// successful subscription. Disconnects caused by the subscriber's own backlog are excluded.
const providerDowntime = downtime('region, cloud_provider, provider',
  `${dataTypeScope} AND ${commitmentScope} AND ${latestStreams}`);
// Providers are compared only within a location; nothing is pooled across
// locations, since not every provider runs everywhere. A region is named with
// its cloud provider only when it has several.
// The tables take their provider columns in first-seen order: alphabetically.
// Rows are sorted by location afterwards.
// win_cell carries the win rate's color tier for the table, since Grafana colors
// cells per column: W = the location's leader (ties all lead), Y = ≥10%, R = <10%.
const shownWin = 'toFloat64(toDecimalString(win_percent, 1))';
// A provider disconnected for the whole range has no summaries, only downtime, so
// the full join keeps it; its metric columns are Nullable and stay NULL (blank).
export const locationPairs = withRollups(`SELECT * EXCEPT (region, cloud_provider),
 if(win_percent IS NULL, NULL, concat(multiIf(
   ${shownWin} = max(${shownWin}) OVER (PARTITION BY region, cloud_provider), 'W', ${shownWin} >= 10, 'Y', 'R'),
   '|', toDecimalString(win_percent, 1))) AS win_cell
 FROM (SELECT
 concat(region, if(uniqExact(cloud_provider) OVER (PARTITION BY region) > 1, concat('/', cloud_provider), '')) AS location,
 region, cloud_provider, provider, win_percent, p50_lag_ms, p99_lag_ms, dropped_percent,
 disconnected_seconds, disconnects
 FROM (SELECT region, cloud_provider, provider, quantilesTDigestIfMerge(0.5, 0.99)(percentiles) AS qs,
 sum(wins) / nullIf(sum(sum(wins)) OVER (PARTITION BY region, cloud_provider), 0) * 100 AS win_percent,
 if(sum(delivered) > 0, qs[1] / 1e6, NULL) AS p50_lag_ms,
 if(sum(delivered) > 0, qs[2] / 1e6, NULL) AS p99_lag_ms,
 sum(missed) / nullIf(sum(delivered) + sum(missed), 0) * 100 AS dropped_percent
 FROM (${rollups}) GROUP BY region, cloud_provider, provider) AS metrics
 -- Providers without recorded disconnects get zero downtime (non-Nullable defaults).
 FULL JOIN (${providerDowntime}) AS downtime USING (region, cloud_provider, provider))
 ORDER BY provider, location`)
  + '\n SETTINGS max_threads = 16, max_execution_time = 45';

type Steps = Array<[number, string]>;
const metrics: Array<[string, string, string, number, string, Steps | null]> = [
  ['win_cell', 'Win rate', 'percent', 1,
    'Share of wins within each location: the first on-time delivery of an event that at least two connected providers competed for. Missing an event’s deadline loses it. Each row sums to 100%. Green = the location’s leader (ties share it), yellow ≥10%, red <10%.',
    null],
  ['p50_lag_ms', 'p50 lag behind ideal', 'ms', 2,
    'Median on-time lag behind each event’s earliest provider arrival at that location. Lower is better; events that missed the deadline are excluded.',
    // Green only when the cell shows 0.00 ms.
    [[0.005, '#EAB839'], [1, '#FF9830'], [5, '#F2495C']]],
  ['p99_lag_ms', 'p99 lag behind ideal', 'ms', 1,
    '99th percentile on-time lag behind each event’s earliest provider arrival at that location. Lower is better; events that missed the deadline are excluded.',
    [[10, '#EAB839'], [20, '#FF9830'], [100, '#F2495C']]],
  ['dropped_percent', 'Missed deadline', 'percent', 2,
    'Share of a connected provider’s events not delivered within the configured window after the first provider’s delivery, even if delivery arrives later.',
    [[0.1, '#EAB839'], [0.5, '#FF9830'], [1, '#F2495C']]],
  ['disconnected_seconds', 'Downtime', 's', 1,
    `Time each provider’s streams at that location could not reconnect: outages lasting at least ${OUTAGE_SECONDS} s, from a disconnect until the next successful subscription, summed over streams. A shorter interruption, such as a rolling restart that accepts the reconnect right away, counts only as a disconnect. Disconnects caused by the subscriber’s own backlog are excluded. A stalled stream counts only from the moment it is dropped for inactivity (30 s without traffic); events in the stall count as missed deadlines.`,
    [[0.001, '#EAB839'], [10, '#FF9830'], [60, '#F2495C']]],
  ['disconnects', 'Disconnects', 'none', 0,
    'How often each provider’s streams at that location were interrupted, including short reconnects that are not downtime. Disconnects caused by the subscriber’s own backlog are excluded.',
    [[1, '#EAB839'], [3, '#FF9830'], [10, '#F2495C']]],
];

const winTiers: Array<[string, string]> = [['W', '#56A64B'], ['Y', '#EAB839'], ['R', '#F2495C']];

// Two tabs (rows here; the deploy turns them into tabs), each with one table
// centered on top and two side by side below it.
const tabs: Array<[number, string]> = [[30, 'Performance'], [31, 'Reliability']];
const layout = [0, 23].flatMap(top => [
  { x: 6, y: top + 1, w: 12, h: 11 }, { x: 0, y: top + 12, w: 12, h: 11 }, { x: 12, y: top + 12, w: 12, h: 11 },
]);

// One heat map per metric: a row per location and a column per provider.
metrics.forEach(([field, name, unit, decimals, description, steps], index) => {
  const panel = new Table().id(20 + index).title(name).description(description)
    .datasource(datasource).gridPos(layout[index])
    .withTarget(new ClickHouseQueryBuilder(locationPairs))
    .withTransformation({ id: 'groupingToMatrix', options: {
      rowField: 'location', columnField: 'provider', valueField: field, emptyValue: 'null',
    } })
    .withTransformation({ id: 'sortBy', options: { sort: [{ field: 'location\\provider' }] } })
    .unit(unit).decimals(decimals).min(0).align('center' as never).minWidth(70)
    // Providers not running at a location stay blank instead of looking like a result.
    .mappings([{ type: MappingType.SpecialValue, options: { match: SpecialValueMatch.NullAndNan, result: { text: '—', color: 'transparent' } } },
      ...(steps ? [] : winTiers.map(([tier, color]) => ({
        type: MappingType.RegexToText as const, options: { pattern: `^${tier}\\|(.*)$`, result: { text: '$1%', color } },
      })))])
    .cellOptions({ type: TableCellDisplayMode.ColorBackground, mode: TableCellBackgroundDisplayMode.Basic })
    .overrideByName('location\\provider', [
      { id: 'displayName', value: 'Location' },
      { id: 'custom.cellOptions', value: { type: TableCellDisplayMode.Auto } },
      { id: 'custom.align', value: 'left' },
    ]);
  if (steps) {
    panel.colorScheme(new FieldColorBuilder().mode(FieldColorModeId.Thresholds))
      .thresholds(new ThresholdsConfigBuilder().mode(ThresholdsMode.Absolute)
        .steps([{ value: null as unknown as number, color: '#56A64B' }, ...steps.map(([value, color]) => ({ value, color }))]));
  }
  overviewDashboard.withPanel(panel);
});
// The first table runs the query; the others reuse its frame.
for (const item of overviewDashboard.build().panels ?? []) {
  const panel = item as Panel;
  const index = (panel.id ?? 0) - 20;
  const fields = ['location', 'provider', metrics[index][0]];
  if (index === 0) selectFields({ build: () => panel }, fields);
  else reuseResults({ build: () => panel }, 20, fields);
}
tabs.forEach(([id, title], index) => overviewDashboard.build().panels?.push(
  new RowBuilder(title).id(id).gridPos({ x: 0, y: index * 23, w: 24, h: 1 }).build()));
