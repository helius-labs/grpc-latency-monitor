import { writeFileSync } from 'node:fs';
import { DashboardLinkBuilder, DashboardLinkType } from '@grafana/grafana-foundation-sdk/dashboard';
import { providersDashboard } from './providers.js';
import { headToHeadDashboard } from './head-to-head.js';
import { overviewDashboard } from './overview.js';
import { dashboardFiles } from './files.js';
import { addSubscriptionPanels, collapseInstructions } from './shared.js';

// The overview stays minimal: no instructions or configuration table.
const builders = [providersDashboard, headToHeadDashboard, overviewDashboard];
for (const builder of [providersDashboard, headToHeadDashboard]) {
  collapseInstructions(builder, 10);
  addSubscriptionPanels(builder);
}
for (const [index, builder] of builders.entries()) {
  const dashboard = builder.build();
  dashboard.panels?.sort((a, b) => (a.gridPos?.y ?? 0) - (b.gridPos?.y ?? 0) || (a.gridPos?.x ?? 0) - (b.gridPos?.x ?? 0));
  dashboard.links = builders.filter(other => other !== builder).map(other => other.build()).map(other =>
    new DashboardLinkBuilder(other.title ?? 'Benchmark dashboard')
      .type(DashboardLinkType.Link).url(`/d/${other.uid}`)
      .includeVars(true).keepTime(true).build());
  const json = JSON.stringify(dashboard, null, 2);
  // Tables resolve against the datasource's default database, so users can keep
  // benchmark data in any database. Never generate database-qualified names.
  const qualified = json.match(/\b\w+\.benchmark_\w+/g);
  if (qualified) throw new Error(`Database-qualified tables in ${dashboard.uid}: ${[...new Set(qualified)].join(', ')}`);
  const output = new URL(dashboardFiles[index], import.meta.url);
  writeFileSync(output, json + '\n');
  console.log(`Generated ${output.pathname}`);
}
