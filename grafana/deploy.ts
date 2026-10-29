import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dashboardFiles, tabbedDashboards } from './files.js';

// Deploys through gcx using its current context; GRAFANA_SERVER, GRAFANA_TOKEN
// (or GRAFANA_USER and GRAFANA_PASSWORD) override that context's fields.
const v1 = 'dashboard.grafana.app/v1';
const v2 = 'dashboard.grafana.app/v2';

function gcx(args: string[], input?: unknown) {
  const result = spawnSync('gcx', [...args, '-o', 'json'], {
    input: input === undefined ? undefined : JSON.stringify(input),
    encoding: 'utf8',
  });
  if (result.error) throw new Error(`Cannot run gcx (is it installed?): ${result.error.message}`);
  if (result.status !== 0) throw new Error(`gcx ${args.join(' ')} failed: ${result.stdout}${result.stderr}`);
  return JSON.parse(result.stdout);
}

function getDashboard(uid: string, apiVersion: string) {
  try {
    return gcx(['dashboards', 'get', uid, '--api-version', apiVersion]);
  } catch (error) {
    if (/not found/i.test(String(error))) return undefined;
    throw error;
  }
}

type Resource = { metadata: { name: string; managedFields?: unknown }; status?: unknown; [key: string]: unknown };

function saveDashboard(resource: Resource, apiVersion: string, create = false) {
  const { status, ...body } = resource;
  delete body.metadata.managedFields;
  const args = create ? ['dashboards', 'create'] : ['dashboards', 'update', resource.metadata.name];
  return gcx([...args, '-f', '-', '--api-version', apiVersion], body);
}

function preserveDashboardPresentation(dashboard: {
  uid: string;
  annotations?: { list?: Array<{ name: string; placement?: string }> };
  panels?: Array<{ type: string; title?: string }>;
}) {
  const placements = new Map<string, string>();
  for (const annotation of dashboard.annotations?.list ?? []) {
    if (annotation.placement) placements.set(annotation.name, annotation.placement);
  }
  const hasInstructions = dashboard.panels?.some(panel => panel.type === 'row' && panel.title === 'How to read these results');
  const tabbed = tabbedDashboards.has(dashboard.uid);
  if (!placements.size && !hasInstructions && !tabbed) return;

  // Grafana 13's v1 conversion puts annotation placement in legacyOptions,
  // while the v2 interface reads spec.placement. Correct the converted
  // resource so subsequent deployments preserve the visible control.
  const resource = getDashboard(dashboard.uid, v2);
  if (!resource) return;
  // Dashboards stored as v0/v1 keep placement in the legacy JSON; saving
  // through v2 there converts back and drops it.
  if (!resource.status?.conversion?.storedVersion?.startsWith('v2')) return;
  let changed = false;
  for (const annotation of resource.spec.annotations ?? []) {
    const placement = placements.get(annotation.spec.name);
    if (placement && annotation.spec.placement !== placement) {
      annotation.spec.placement = placement;
      if (annotation.spec.legacyOptions) delete annotation.spec.legacyOptions.placement;
      changed = true;
    }
  }
  // The v2 layout can put the instructions first while retaining a separate
  // results grid with no row header. Legacy row ordering would nest that grid.
  if (hasInstructions && resource.spec.layout?.kind === 'RowsLayout') {
    const rows = resource.spec.layout.spec.rows;
    const index = rows.findIndex((row: { spec: { title?: string } }) => row.spec.title === 'How to read these results');
    if (index > 0) {
      rows.unshift(...rows.splice(index, 1));
      changed = true;
    }
  }
  // Legacy JSON has no tabs: its rows convert to a v2 rows layout, which
  // becomes a tabs layout with one tab per row.
  if (tabbed && resource.spec.layout?.kind === 'RowsLayout') {
    resource.spec.layout = { kind: 'TabsLayout', spec: {
      tabs: resource.spec.layout.spec.rows.map((row: { spec: { title?: string; layout: unknown } }) => ({
        kind: 'TabsLayoutTab', spec: { title: row.spec.title, layout: row.spec.layout },
      })),
    } };
    changed = true;
  }
  if (changed) saveDashboard(resource, v2);
}

for (const filename of dashboardFiles) {
  const dashboard = JSON.parse(readFileSync(new URL(filename, import.meta.url), 'utf8'));
  const { id, version, ...spec } = dashboard;
  // Updating the fetched resource keeps its folder and resourceVersion.
  const existing = getDashboard(dashboard.uid, v1);
  if (existing) saveDashboard({ ...existing, spec }, v1);
  else saveDashboard({ apiVersion: v1, kind: 'Dashboard', metadata: { name: dashboard.uid }, spec }, v1, true);
  preserveDashboardPresentation(dashboard);
  console.log(`Dashboard saved: ${dashboard.uid}`);
}
