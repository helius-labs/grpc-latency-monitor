import { DashboardCursorSync, AnnotationPanelFilterBuilder, AnnotationQueryBuilder, AnnotationQueryPlacement, FieldColorBuilder, FieldColorModeId, MappingType, ThresholdsConfigBuilder, ThresholdsMode } from '@grafana/grafana-foundation-sdk/dashboard';
import { PanelBuilder as Stat } from '@grafana/grafana-foundation-sdk/stat';
import { BigValueGraphMode, BigValueColorMode, BigValueJustifyMode, BigValueTextMode, ReduceDataOptionsBuilder } from '@grafana/grafana-foundation-sdk/common';
import { ClickHouseQueryBuilder, datasource } from './clickhouse.js';
import { baseDashboard, addVariables, note, trend, referenceLine, selectFields, reuseResults } from './shared.js';
import * as sql from './queries.js';

export const headToHeadDashboard = baseDashboard('Solana Benchmark — Head to Head', 'solana-streaming-head-to-head')
  .tooltip(DashboardCursorSync.Crosshair);
addVariables(headToHeadDashboard, true);
for (const [name, color, query] of [
  ['Connections', '#00B5C3', sql.pairConnectedEvents],
  ['Disconnections', '#F2495C', sql.pairDisconnectedEvents],
]) {
  headToHeadDashboard.annotation(new AnnotationQueryBuilder()
    .name(name).datasource(datasource).enable(true).hide(false)
    .placement(AnnotationQueryPlacement.InControlsMenu)
    .iconColor(color)
    .filter(new AnnotationPanelFilterBuilder().exclude(false).ids([3, 4, 6]))
    .target(new ClickHouseQueryBuilder(query)));
}
headToHeadDashboard
  .withPanel(note([
    '**Resolution:** ten-second detail for the latest six hours; minute summaries for longer or older views. Overview percentiles are exact up to one hour and approximate for longer views. Counts and wins stay exact. Ranges older than six hours are widened to whole minutes at their edges (raw detail has expired). Summary, connection and health history retain 30 days.',
    'Compare **${provider_a} (A)** and **${provider_b} (B)** for the same region, cloud provider and subscription. We compare **the same event delivered on time by both providers**. **All** pools account, transaction and slot events, using the latest configuration per stream.',
    '**Missed deadline = not delivered within the configured window.** Each event opens the configured comparison window at the first provider’s delivery. A connected provider that has not delivered by then records a **missed deadline** for that event, even if its delivery arrives later. Events that missed the deadline are not part of the differences, but they count in win rates: delivering on time when the other provider misses the deadline is a win.',
    `**Downtime:** events while a provider is disconnected are excluded from the comparison, not counted as missed deadlines. The **downtime** cards show outages of at least ${sql.OUTAGE_SECONDS} s, when reconnecting kept failing; quicker reconnects are not downtime.`,
    'The difference is **${provider_a} arrival time − ${provider_b} arrival time**, in milliseconds. **Negative** means ${provider_a} arrived earlier; **positive** means ${provider_b} arrived earlier.',
    'Sort these signed differences from smallest to largest: **p5** is the 5th percentile, **p50** is the median (half the differences are below it, half above), and **p95** is the 95th percentile. **p0.1** is the 0.1st percentile. These describe arrival differences, not a provider’s win rate.',
    '**Illustrative examples (not live readings):**\n\n- **p5 = −10 ms:** for roughly 5% of paired events, ${provider_a} arrived at least 10 ms before ${provider_b}.\n- **p95 = +20 ms:** roughly 95% of differences are at or below +20 ms; the remaining 5% are events where ${provider_a} arrived more than 20 ms after ${provider_b}.',
    'The cards summarize the **selected time range**; the graph calculates percentiles for **each time bucket**. **Avg** is the mean signed difference. Each win rate is **that provider’s wins / (${provider_a} wins + ${provider_b} wins)**.',
  ].join('\n\n'), 4))
  .withPanel(referenceLine(trend(3, 'Same-event arrival difference: ${provider_a} − ${provider_b}', sql.pairTrends, 0, 12, 'ms'), 0)
    .gridPos({ x: 0, y: 12, w: 24, h: 10 })
    .description('Paired on-time arrival difference (${provider_a} − ${provider_b}), per bucket. Negative: ${provider_a} faster. Positive: ${provider_b} faster.')
    .overrideByName('avg_a_minus_b_ms', [{ id: 'displayName', value: 'avg' }])
    .overrideByName('p1_a_minus_b_ms', [{ id: 'displayName', value: 'p1' }])
    .overrideByName('p5_a_minus_b_ms', [{ id: 'displayName', value: 'p5' }])
    .overrideByName('median_a_minus_b_ms', [{ id: 'displayName', value: 'p50' }])
    .overrideByName('p95_a_minus_b_ms', [{ id: 'displayName', value: 'p95' }])
    .overrideByName('p99_a_minus_b_ms', [{ id: 'displayName', value: 'p99' }]))
  .withPanel(referenceLine(trend(4, 'Provider win rates', null, 0, 22, 'percent'), 50)
    .gridPos({ x: 0, y: 22, w: 12, h: 8 })
    .axisSoftMin(0).axisSoftMax(100)
    .description('Provider wins / (${provider_a} wins + ${provider_b} wins), per bucket. A win is the faster on-time delivery, or an on-time delivery when the other provider misses the deadline. Ties excluded; dashed line = 50%.')
    .overrideByName('a_win_percent', [{ id: 'displayName', value: '${provider_a}' }])
    .overrideByName('b_win_percent', [{ id: 'displayName', value: '${provider_b}' }]))
  .withPanel(trend(6, 'Missed deadlines per second', sql.pairDroppedRate, 12, 22, 'ops')
    .gridPos({ x: 12, y: 22, w: 12, h: 8 })
    .axisSoftMin(0)
    .description('Events each provider did not deliver within the configured window after the first provider’s delivery while connected, per second, including unpaired events. Deliveries after that window still count as missed deadlines. Gaps mean no observations.')
    .displayName('${__field.labels.series}'));

const cards = [
  { title: 'p0.1', field: 'p001_ms', x: 0, y: 4, w: 4, color: '#398BFF' },
  { title: 'p1', field: 'p1_ms', x: 4, y: 4, w: 3, color: '#75B4FF' },
  { title: 'p5', field: 'p5_ms', x: 7, y: 4, w: 3, color: '#BCDDFF' },
  { title: 'p50 (median)', field: 'p50_ms', x: 10, y: 4, w: 4, color: '#F2F2F5' },
  { title: 'p95', field: 'p95_ms', x: 14, y: 4, w: 3, color: '#DFC5FF' },
  { title: 'p99', field: 'p99_ms', x: 17, y: 4, w: 3, color: '#B78AFF' },
  { title: 'p99.9', field: 'p999_ms', x: 20, y: 4, w: 4, color: '#9354ED' },
  { title: 'Avg', field: 'avg_ms', x: 0, y: 8, w: 4, color: '#73BF69' },
  { title: '${provider_a} win rate', field: 'a_win_percent', x: 12, y: 8, w: 6, color: '#A9A9B4', winRate: true },
  { title: '${provider_b} win rate', field: 'b_win_percent', x: 18, y: 8, w: 6, color: '#A9A9B4', winRate: true },
];
for (const [index, card] of cards.entries()) {
  const panel = new Stat().id(20 + index).title('').displayName(card.title)
    .description(card.winRate
      ? 'Provider wins / (${provider_a} wins + ${provider_b} wins). A win is the faster on-time delivery, or an on-time delivery when the other provider misses the deadline. Ties excluded. Green >50%, red <50%, gray =50%.'
      : 'Paired on-time arrival difference (${provider_a} − ${provider_b}), over the selected range. Negative: ${provider_a} faster. Positive: ${provider_b} faster.')
    .datasource(datasource).gridPos({ x: card.x, y: card.y, w: card.w, h: 4 })
    .unit(card.winRate ? 'percent' : 'ms').decimals(2).noValue('No data')
    .graphMode(BigValueGraphMode.None)
    .colorMode(card.winRate ? BigValueColorMode.Background : BigValueColorMode.Value)
    .colorScheme(card.winRate
      ? new FieldColorBuilder().mode(FieldColorModeId.Thresholds)
      : new FieldColorBuilder().mode(FieldColorModeId.Fixed).fixedColor(card.color))
    .justifyMode(BigValueJustifyMode.Center).textMode(BigValueTextMode.ValueAndName)
    .reduceOptions(new ReduceDataOptionsBuilder().calcs(['lastNotNull']).values(false));
  if (index === 0) {
    panel.withTarget(new ClickHouseQueryBuilder(sql.pairSummary));
    selectFields(panel, [card.field]);
  }
  else reuseResults(panel, 20, [card.field]);
  if (card.winRate) {
    panel.thresholds(new ThresholdsConfigBuilder().mode(ThresholdsMode.Absolute)
      .steps([{ value: null, color: '#F2495C' }, { value: 50, color: '#73BF69' }]))
      .mappings([{ type: MappingType.ValueToText, options: { '50': { color: '#A9A9B4' } } }]);
  }
  headToHeadDashboard.withPanel(panel);
}

// Downtime cards share one query; events while disconnected are excluded from the
// comparison rather than counted as missed deadlines, so their cost is shown here.
for (const [index, side] of (['a', 'b'] as const).entries()) {
  const panel = new Stat().id(30 + index).title('').displayName(`\${provider_${side}} downtime`)
    .description(`Time this provider’s selected streams could not reconnect: outages lasting at least ${sql.OUTAGE_SECONDS} s, from a disconnect until the next successful subscription, summed over streams. Shorter interruptions, such as a rolling restart that accepts the reconnect right away, are not downtime. Disconnects caused by the subscriber’s own backlog are excluded. Events while disconnected are excluded from the comparison, not counted as missed deadlines.`)
    .datasource(datasource).gridPos({ x: 4 + index * 4, y: 8, w: 4, h: 4 })
    .unit('s').decimals(1).noValue('No data')
    .graphMode(BigValueGraphMode.None).colorMode(BigValueColorMode.Value)
    .colorScheme(new FieldColorBuilder().mode(FieldColorModeId.Fixed).fixedColor('#F2495C'))
    .justifyMode(BigValueJustifyMode.Center).textMode(BigValueTextMode.ValueAndName)
    .reduceOptions(new ReduceDataOptionsBuilder().calcs(['lastNotNull']).values(false));
  if (index === 0) {
    panel.withTarget(new ClickHouseQueryBuilder(sql.pairDowntime));
    selectFields(panel, ['a_disconnected_seconds']);
  } else reuseResults(panel, 30, ['b_disconnected_seconds']);
  headToHeadDashboard.withPanel(panel);
}

const differenceFields = ['time', 'avg_a_minus_b_ms', 'p1_a_minus_b_ms', 'p5_a_minus_b_ms',
  'median_a_minus_b_ms', 'p95_a_minus_b_ms', 'p99_a_minus_b_ms'];
for (const panel of headToHeadDashboard.build().panels ?? []) {
  if (panel.id === 3) selectFields({ build: () => panel }, differenceFields);
  if (panel.id === 4) reuseResults({ build: () => panel }, 3, ['time', 'a_win_percent', 'b_win_percent']);
}
