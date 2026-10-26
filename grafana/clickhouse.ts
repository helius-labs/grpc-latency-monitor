import type { Builder, Dataquery } from '@grafana/grafana-foundation-sdk/cog';
import type { DataSourceRef } from '@grafana/grafana-foundation-sdk/common';

export const datasource: DataSourceRef = {
  type: 'grafana-clickhouse-datasource',
  uid: process.env.CLICKHOUSE_DATASOURCE_UID ?? 'benchmark-clickhouse',
};

// The Foundation SDK has no ClickHouse query builder. This adapter implements
// its typed query extension point using the installed ClickHouse plugin's model.
interface ClickHouseQuery extends Dataquery {
  refId: string;
  datasource: DataSourceRef;
  queryType: 'sql';
  editorType: 'sql';
  rawSql: string;
  format: 0 | 1;
}

export class ClickHouseQueryBuilder implements Builder<ClickHouseQuery> {
  constructor(private readonly sql: string, private readonly format: 0 | 1 = 1) {}

  build(): ClickHouseQuery {
    return {
      _implementsDataqueryVariant() {},
      refId: 'A',
      datasource,
      queryType: 'sql',
      editorType: 'sql',
      rawSql: this.sql,
      format: this.format,
    };
  }
}

// Grafana's built-in datasource subscribes to a source panel's untransformed
// frames; each consumer selects its own fields without querying ClickHouse.
export const dashboardDatasource: DataSourceRef = { type: 'datasource', uid: '-- Dashboard --' };

interface DashboardQuery extends Dataquery {
  refId: string;
  datasource: DataSourceRef;
  panelId: number;
  withTransforms: boolean;
}

export class DashboardQueryBuilder implements Builder<DashboardQuery> {
  constructor(private readonly panelId: number) {}

  build(): DashboardQuery {
    return {
      _implementsDataqueryVariant() {},
      refId: 'A',
      datasource: dashboardDatasource,
      panelId: this.panelId,
      withTransforms: false,
    };
  }
}
