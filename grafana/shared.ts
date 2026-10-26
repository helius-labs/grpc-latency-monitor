import {
  DashboardBuilder, QueryVariableBuilder, VariableHide, VariableRefresh, VariableSort,
  ThresholdsConfigBuilder, ThresholdsMode, RowBuilder,
} from '@grafana/grafana-foundation-sdk/dashboard';
import { PanelBuilder as TimeSeries } from '@grafana/grafana-foundation-sdk/timeseries';
import { PanelBuilder as Table } from '@grafana/grafana-foundation-sdk/table';
import { PanelBuilder as Text, TextMode } from '@grafana/grafana-foundation-sdk/text';
import {
  VizLegendOptionsBuilder, VizTooltipOptionsBuilder, LegendDisplayMode,
  LegendPlacement, TooltipDisplayMode, SortOrder,
  GraphThresholdsStyleConfigBuilder, GraphThresholdsStyleMode,
} from '@grafana/grafana-foundation-sdk/common';
import { ClickHouseQueryBuilder, DashboardQueryBuilder, dashboardDatasource, datasource } from './clickhouse.js';
import { subscriptionOptions, subscriptionDetails, providerOptions } from './subscriptions.js';
import type { Panel } from '@grafana/grafana-foundation-sdk/dashboard';

function variable(dashboard: DashboardBuilder, name: string, label: string, query: string, current?: string, multi = false) {
  const builder = new QueryVariableBuilder(name).label(label)
    .datasource(datasource).query(query).definition(query)
    .refresh(query.includes('$__timeFilter') ? VariableRefresh.OnTimeRangeChanged : VariableRefresh.OnDashboardLoad)
    .sort(VariableSort.AlphabeticalAsc)
    .multi(multi).includeAll(multi);
  if (multi) builder.current({ text: 'All', value: '$__all' });
  else if (current) builder.current({ text: current, value: current });
  dashboard.withVariable(builder);
}

export function baseDashboard(title: string, uid: string) {
  return new DashboardBuilder(title).uid(uid)
    .tags(['solana', 'benchmark']).timezone('').editable()
    .time({ from: 'now-30m', to: 'now' }).refresh('30s');
}

export function addStreamVariables(dashboard: DashboardBuilder) {
  dashboard.withVariable(new QueryVariableBuilder('data_type').label('Stream')
    .description('All pools events using the latest configuration per stream.')
    .datasource(datasource)
    .query('SELECT DISTINCT data_type FROM benchmark_subscription_versions ORDER BY data_type')
    .definition('SELECT DISTINCT data_type FROM benchmark_subscription_versions ORDER BY data_type')
    .refresh(VariableRefresh.OnDashboardLoad).sort(VariableSort.AlphabeticalAsc)
    .multi(false).includeAll(false).allowCustomValue(false)
    .staticOptions([{ text: 'All', value: '__all' }]).staticOptionsOrder('before')
    .current({ text: 'transaction', value: 'transaction' }));
  variable(dashboard, 'commitment', 'Commitment', 'SELECT DISTINCT commitment FROM benchmark_subscription_versions ORDER BY commitment', 'processed');
}

export function addVariables(dashboard: DashboardBuilder, headToHead = false) {
  variable(dashboard, 'region', 'Region', 'SELECT DISTINCT region FROM benchmark_subscriber_locations ORDER BY region', 'local');
  variable(dashboard, 'cloud_provider', 'Cloud provider', 'SELECT DISTINCT cloud_provider FROM benchmark_subscriber_locations WHERE region = ${region:sqlstring} AND cloud_provider != \'\' ORDER BY cloud_provider');
  addStreamVariables(dashboard);
  const automatic = { text: 'Latest (automatic)', value: '__latest' };
  dashboard.withVariable(new QueryVariableBuilder('subscription_id').label('Subscription history')
    .description('Latest configuration in the selected range. History requires a single stream; All uses latest per stream.')
    .datasource(datasource).query(subscriptionOptions).definition(subscriptionOptions)
    .hide(VariableHide.InControlsMenu).refresh(VariableRefresh.OnTimeRangeChanged)
    .sort(VariableSort.Disabled).multi(false).includeAll(false).allowCustomValue(false)
    .staticOptions([automatic]).staticOptionsOrder('before').current(automatic));
  // List all providers seen for this stream in the range. Automatic mode can
  // change configuration without a variable change on each dashboard refresh.
  const providers = providerOptions;
  if (headToHead) {
    // No hardcoded defaults: A is the first provider, B the first other than A.
    variable(dashboard, 'provider_a', 'Provider A', providers);
    variable(dashboard, 'provider_b', 'Provider B',
      `SELECT provider FROM (${providers}) WHERE provider != \${provider_a:sqlstring} ORDER BY provider`);
  } else {
    variable(dashboard, 'provider', 'Providers', providers, undefined, true);
  }
}

export function collapseInstructions(dashboard: DashboardBuilder, expandedHeight?: number) {
  const built = dashboard.build();
  const instructions = built.panels?.find(panel => panel.id === 1 && panel.type === 'text') as Panel | undefined;
  if (!instructions?.gridPos) return;
  const height = instructions.gridPos.h;
  built.panels = built.panels?.filter(panel => panel !== instructions);
  for (const panel of built.panels ?? []) {
    if (panel.gridPos && panel.gridPos.y >= height) panel.gridPos.y -= height;
  }
  const bottom = Math.max(0, ...(built.panels ?? []).map(panel => (panel.gridPos?.y ?? 0) + (panel.gridPos?.h ?? 0)));
  instructions.gridPos.y = bottom + 1;
  instructions.gridPos.h = expandedHeight ?? height;
  // Grafana groups following panels into a row until the next row. Keep this
  // collapsed row after the results so it contains only the instructions.
  built.panels?.push(new RowBuilder('How to read these results').id(83).collapsed(true)
    .gridPos({ x: 0, y: bottom, w: 24, h: 1 })
    .withPanel({ build: () => instructions }).build());
}

export function addSubscriptionPanels(dashboard: DashboardBuilder) {
  const built = dashboard.build();
  const bottom = Math.max(0, ...(built.panels ?? []).map(panel => (panel.gridPos?.y ?? 0) + (panel.gridPos?.h ?? 0)));
  dashboard.withRow(new RowBuilder('Subscription details').id(81).collapsed(true)
    .gridPos({ x: 0, y: bottom, w: 24, h: 1 })
    .withPanel(table(82, 'Configurations observed in this time range', subscriptionDetails, 0, bottom + 1, 24, 7,
      'Observed configurations and filters within the selected range. Choose history under Controls → Subscription history.')
      .overrideByName('stream', [{ id: 'displayName', value: 'Stream' }])
      .overrideByName('configuration', [{ id: 'displayName', value: 'Configuration' }])
      .overrideByName('first_seen', [{ id: 'displayName', value: 'First observed (in range)' }])
      .overrideByName('last_seen', [{ id: 'displayName', value: 'Last observed (in range)' }])
      .overrideByName('filters', [{ id: 'displayName', value: 'Full filters' }])
      .overrideByName('subscription_id', [{ id: 'displayName', value: 'Full subscription ID' }])));
}

export function note(content: string, height = 3) {
  return new Text().id(1).title('How to read these results')
    .gridPos({ x: 0, y: 0, w: 24, h: height }).mode(TextMode.Markdown).content(content);
}

export function table(id: number, title: string, query: string, x: number, y: number, w: number, h: number, description = '') {
  return new Table().id(id).title(title).description(description)
    .datasource(datasource).gridPos({ x, y, w, h }).showHeader(true)
    .withTarget(new ClickHouseQueryBuilder(query));
}
// A null query marks a panel that reuses another panel's results (see reuseResults).
export function trend(id: number, title: string, query: string | null, x: number, y: number, unit: string) {
  const panel = new TimeSeries().id(id).title(title).datasource(datasource)
    .gridPos({ x, y, w: 12, h: 8 }).unit(unit)
    .interval('10s').maxDataPoints(2160)
    .lineWidth(2).fillOpacity(0).spanNulls(false)
    .legend(new VizLegendOptionsBuilder().showLegend(true).displayMode(LegendDisplayMode.Table)
      .placement(LegendPlacement.Bottom).calcs(['lastNotNull', 'mean']))
    .tooltip(new VizTooltipOptionsBuilder().mode(TooltipDisplayMode.Multi).sort(SortOrder.Descending));
  return query === null ? panel : panel.withTarget(new ClickHouseQueryBuilder(query, 0));
}

export function referenceLine(panel: TimeSeries, value: number) {
  return panel.axisSoftMin(value).axisSoftMax(value)
    .thresholds(new ThresholdsConfigBuilder().mode(ThresholdsMode.Absolute)
      .steps([{ value: null, color: '#A9A9B4' }, { value, color: '#A9A9B4' }]))
    .thresholdsStyle(new GraphThresholdsStyleConfigBuilder().mode(GraphThresholdsStyleMode.Dashed));
}

// Keep source panels visible and reuse raw frames, so their display transforms
// cannot remove metrics needed by consumers.
export function selectFields<T extends { build(): Panel }>(panel: T, fields: string[]): T {
  const built = panel.build();
  built.transformations = [{ id: 'filterFieldsByName', options: {
    include: { names: fields },
  } }, ...(built.transformations ?? [])];
  return panel;
}

export function reuseResults<T extends { build(): Panel }>(panel: T, sourcePanelId: number, fields: string[]): T {
  const built = panel.build();
  built.datasource = dashboardDatasource;
  built.targets = [new DashboardQueryBuilder(sourcePanelId).build()];
  return selectFields(panel, fields);
}
