import {
  type Panel, DashboardCursorSync, AnnotationPanelFilterBuilder, AnnotationQueryBuilder,
  AnnotationQueryPlacement, FieldColorBuilder, FieldColorModeId,
} from '@grafana/grafana-foundation-sdk/dashboard';
import { PanelBuilder as BarGauge } from '@grafana/grafana-foundation-sdk/bargauge';
import {
  VizOrientation, BarGaugeDisplayMode, BarGaugeValueMode, BarGaugeNamePlacement,
  BarGaugeSizing, ReduceDataOptionsBuilder, VizLegendOptionsBuilder,
} from '@grafana/grafana-foundation-sdk/common';
import { baseDashboard, addVariables, note, trend, referenceLine, selectFields, reuseResults } from './shared.js';
import { ClickHouseQueryBuilder, datasource } from './clickhouse.js';
import * as sql from './queries.js';

// Preserve the original UID so existing bookmarks open the provider overview.
export const providersDashboard = baseDashboard('Solana Benchmark — All Providers', 'solana-streaming-benchmark')
  .tooltip(DashboardCursorSync.Crosshair);
addVariables(providersDashboard);
for (const [name, color, query] of [
  ['Connections', '#00B5C3', sql.connectedEvents],
  ['Disconnections', '#F2495C', sql.disconnectedEvents],
]) {
  providersDashboard.annotation(new AnnotationQueryBuilder()
    .name(name).datasource(datasource).enable(true).hide(false)
    .placement(AnnotationQueryPlacement.InControlsMenu)
    .iconColor(color)
    .filter(new AnnotationPanelFilterBuilder().exclude(false).ids([3, 4, 11, 12, 13, 5, 6, 10]))
    .target(new ClickHouseQueryBuilder(query)));
}

const winDescription = 'Provider wins / selected-provider wins. A win is the first on-time delivery of an event that ≥2 connected providers competed for; dropping an event loses it. First-place ties excluded. All configured providers compete.';
const downtimeDescription = `Downtime: time each provider’s selected streams could not reconnect, counting outages that lasted at least ${sql.OUTAGE_SECONDS} s from a disconnect until the next successful subscription, summed over streams. Disconnects: how often the streams were interrupted, including short reconnects (such as a rolling restart) that are not downtime. Disconnects caused by the subscriber’s own backlog (a full arrival or control queue) are excluded. Events while disconnected are excluded from comparisons rather than counted as dropped, so check downtime and disconnects alongside win and dropped rates.`;
const lagDescription = 'On-time lag behind each event’s earliest configured-provider arrival (ms), within the 5 s window. Lower is better; dropped events are excluded and provider samples can differ.';

function comparison(id: number, title: string, query: string, x: number, unit: string, color: string, description: string, valueField = unit === 'percent' ? 'win_percent' : 'lag_ms') {
  return new BarGauge().id(id).title(title).description(description)
    .datasource(datasource).gridPos({ x, y: 4, w: 8, h: 7 })
    .withTarget(new ClickHouseQueryBuilder(query))
    .withTransformation({ id: 'rowsToFields', options: { mappings: [
      { fieldName: 'provider', handlerKey: 'field.name' },
      { fieldName: valueField, handlerKey: 'field.value' },
    ] } })
    .orientation(VizOrientation.Horizontal).displayMode(BarGaugeDisplayMode.Basic)
    .namePlacement(BarGaugeNamePlacement.Left).valueMode(BarGaugeValueMode.Text)
    .showUnfilled(true).sizing(BarGaugeSizing.Manual).minVizHeight(28).maxVizHeight(28)
    .reduceOptions(new ReduceDataOptionsBuilder().calcs(['lastNotNull']).values(false))
    .unit(unit).decimals(2).min(0).noValue('No data')
    .colorScheme(new FieldColorBuilder().mode(FieldColorModeId.Fixed).fixedColor(color))
    .legend(new VizLegendOptionsBuilder().showLegend(false));
}

providersDashboard
  .withPanel(note([
    '**Resolution:** ten-second detail for the latest six hours; minute summaries for longer or older views. Overview percentiles are exact up to one hour and approximate for longer views. Counts and wins stay exact. Ranges older than six hours are widened to whole minutes at their edges (raw detail has expired). Summary, connection and health history retain 30 days.',
    'Bars summarize the selected time range. Graphs show each time bucket. Bars and trends use the selected region and cloud provider. **All** pools account, transaction and slot events, using the latest configuration per stream.',
    '**Ideal provider:** the first arrival for each event across all configured providers. The winner can change each event. Lag is the delay behind that arrival, in milliseconds. Hiding a provider does not change this baseline. Example: A arrives first, B follows 4 ms later → A = 0 ms, B = 4 ms. This measures local delivery differences, not time since the event was produced.',
    '**Dropped = not delivered within 5 seconds.** Each event opens a 5 s window at the first provider’s delivery. A connected provider that has not delivered by then counts as **dropped** for that event, even if its delivery arrives later. Later deliveries are ignored; there is no “late” category.',
    '**Percentiles:** lower is better. **p50 = 4 ms** means roughly half of a provider’s on-time deliveries lagged the ideal by 4 ms or less. **p99 = 20 ms** means roughly 99% lagged by 20 ms or less. p95 and p99.9 follow the same rule. Dropped events are excluded, so read percentiles together with the dropped rate.',
    '**Comparing providers:** percentile samples can differ. Subtracting two p50 values does not tell you the median difference on shared events. Use **Head to Head** for that comparison.',
    '**Win rate:** provider wins ÷ combined wins of selected providers. A win is the first on-time delivery of an event that at least two connected providers competed for; a provider that **drops** an event loses it to every provider that delivered it. First-place ties are excluded. Example: A wins 60 times and B wins 40 → 60% and 40%. Hidden providers still compete. Shares sum to 100%; no selected wins means no data.',
    `**Downtime:** events while a provider is disconnected are excluded from its comparisons, not counted as dropped. **Downtime** counts only outages of at least ${sql.OUTAGE_SECONDS} s, when reconnecting kept failing; **Disconnects** counts every interruption, including quick reconnects.`,
    '**Dropped rates and controls:** dropped rates are raw observations of connected providers; time while a provider is disconnected or coverage is unknown is not counted as dropped. Toggle markers under **Controls → Connections / Disconnections**. Connection status alone does not prove healthy coverage. Hover any graph to sync the time cursor.',
  ].join('\n\n'), 4))
  .withPanel(comparison(20, 'Provider win rates', sql.providerWinSummary, 0, 'percent', '#73BF69', winDescription).max(100))
  .withPanel(comparison(21, 'p50 (median) lag behind ideal provider', sql.providerLagSummaries, 8, 'ms', '#75B4FF', lagDescription, 'p50_lag_ms'))
  .withPanel(comparison(22, 'p99 lag behind ideal provider', sql.providerLagSummaries, 16, 'ms', '#B78AFF', lagDescription, 'p99_lag_ms'))
  .withPanel(referenceLine(trend(3, 'p50 (median) lag behind ideal provider', sql.providerMetricsTrend, 0, 11, 'ms'), 0)
    .description(lagDescription).displayName('${__field.labels.provider}'))
  .withPanel(referenceLine(trend(4, 'p95 lag behind ideal provider', null, 0, 19, 'ms'), 0)
    .gridPos({ x: 0, y: 19, w: 8, h: 8 })
    .description(lagDescription).displayName('${__field.labels.provider}'))
  .withPanel(referenceLine(trend(11, 'p99 lag behind ideal provider', null, 8, 19, 'ms'), 0)
    .gridPos({ x: 8, y: 19, w: 8, h: 8 })
    .description(lagDescription).displayName('${__field.labels.provider}'))
  .withPanel(referenceLine(trend(12, 'p99.9 lag behind ideal provider', null, 16, 19, 'ms'), 0)
    .gridPos({ x: 16, y: 19, w: 8, h: 8 })
    .description(lagDescription).displayName('${__field.labels.provider}'))
  .withPanel(trend(13, 'Provider win rates over time', sql.providerWinTrend, 12, 11, 'percent')
    .gridPos({ x: 12, y: 11, w: 12, h: 8 }).axisSoftMin(0).axisSoftMax(100)
    .description(winDescription).displayName('${__field.labels.provider}'))
  .withPanel(trend(5, 'Observed on-time events per second', null, 0, 27, 'ops')
    .gridPos({ x: 0, y: 27, w: 8, h: 8 })
    .displayName('${__field.labels.provider}'))
  .withPanel(trend(6, 'Dropped events per second (not delivered within 5 s)', null, 16, 27, 'ops')
    .gridPos({ x: 16, y: 27, w: 8, h: 8 }).axisSoftMin(0)
    .description('Events a connected provider did not deliver within 5 s of the first provider’s delivery, per second. Deliveries after 5 s still count as dropped. Raw, deduplicated observations; gaps mean no observations.')
    .displayName('${__field.labels.provider}'))
  .withPanel(trend(10, 'On-time message throughput — ${data_type} / ${commitment}', null, 8, 27, 'Bps')
    .gridPos({ x: 8, y: 27, w: 8, h: 8 })
    .description('On-time protobuf bytes/s for accounts and transactions. Excludes transport overhead; unknown sizes omitted.')
    .displayName('${__field.labels.provider}'))
  .withPanel(comparison(23, 'Downtime', sql.providerDowntime, 0, 's', '#F2495C', downtimeDescription, 'disconnected_seconds')
    .gridPos({ x: 0, y: 35, w: 12, h: 7 }).decimals(1))
  .withPanel(comparison(24, 'Disconnects', sql.providerDowntime, 12, 'none', '#FF9830', downtimeDescription, 'disconnects')
    .gridPos({ x: 12, y: 35, w: 12, h: 7 }).decimals(0));

// The p50 bar and trend are visible source panels. Consumers reuse raw frames
// and filter before their own visualization transformations.
const trendMetrics = new Map<number, string[]>([
  [3, ['p50_lag_ms']], [4, ['p95_lag_ms']], [11, ['p99_lag_ms']], [12, ['p999_lag_ms']],
  [5, ['events_per_second']], [6, ['dropped_per_second']], [10, ['bytes_per_second']],
]);
for (const item of providersDashboard.build().panels ?? []) {
  const panel = item as Panel;
  // Paired bars share one query: the first is the visible source, the second reuses it.
  for (const [source, consumer, sourceMetric, consumerMetric] of [
    [21, 22, 'p50_lag_ms', 'p99_lag_ms'], [23, 24, 'disconnected_seconds', 'disconnects'],
  ] as const) {
    if (panel.id !== source && panel.id !== consumer) continue;
    const metric = panel.id === source ? sourceMetric : consumerMetric;
    if (panel.id === source) selectFields({ build: () => panel }, ['provider', metric]);
    else reuseResults({ build: () => panel }, source, ['provider', metric]);
    panel.transformations?.splice(1, 0, { id: 'sortBy', options: { sort: [{ field: metric }] } });
  }
  const metrics = trendMetrics.get(panel.id ?? -1);
  if (metrics) {
    if (panel.id === 3) selectFields({ build: () => panel }, ['time', ...metrics]);
    else reuseResults({ build: () => panel }, 3, ['time', ...metrics]);
  }
}
