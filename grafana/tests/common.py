"""Shared helpers for the dashboard SQL tests.

The tests run the dashboards' actual generated SQL in clickhouse-local against
small Memory-table fixtures. Fixture rows are written with timestamps on
2026-10-08 for readability; `shift` moves every such timestamp to a few hours
before now, because the dashboards read raw rows only within the six-hour raw
retention.
"""
import datetime as dt
import json
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_START = dt.datetime(2026, 10, 8, tzinfo=dt.timezone.utc)
ANCHOR = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=3)


def tsx(program):
    """Run a TypeScript snippet from the repository root and parse its JSON output."""
    return json.loads(subprocess.check_output(
        [str(ROOT / 'grafana/node_modules/.bin/tsx'), '-e', program], cwd=ROOT, text=True))


# Every dashboard query, plus `exactQueries`: the raw-row forms of the routed ones.
QUERIES = tsx("""
import * as q from './grafana/queries.ts';
import * as s from './grafana/subscriptions.ts';
import { locationPairs } from './grafana/overview.ts';
console.log(JSON.stringify({ ...q, ...s, overview: locationPairs }));
""")


def rollup_sql(name):
    return (ROOT / 'sql/rollups' / f'{name}.sql').read_text()


def build_sql():
    """The rollup worker's minute build, as src/rollups.rs assembles it."""
    return 'INSERT INTO benchmark_minute_rollups\n' + rollup_sql('build').replace(
        '{{source_filter}}', "region = {region:String} AND cloud_provider = {cloud_provider:String}\n "
        "AND event_utc >= {bucket:DateTime64(9, 'UTC')} AND event_utc < {bucket:DateTime64(9, 'UTC')} + INTERVAL 1 MINUTE"
    ).replace('{{bucket}}', "{bucket:DateTime64(9, 'UTC')}").replace('{{generation}}', '{generation:UInt64}')


def migration_ddl(pattern, engine=None):
    """CREATE statements from the migrations whose table name matches `pattern`,
    optionally with their engine (and everything after it) replaced."""
    statements = []
    for path in sorted((ROOT / 'sql/migrations').glob('*.sql')):
        up = re.sub(r'--[^\n]*', '', path.read_text().split('-- +goose Down')[0])
        for sql in up.split(';'):
            match = re.match(r'\s*CREATE (?:MATERIALIZED VIEW|TABLE) (\w+)', sql)
            if match and re.fullmatch(pattern, match.group(1)):
                if engine and 'ENGINE' in sql:
                    sql = sql[:sql.index('ENGINE')] + 'ENGINE = ' + engine
                statements.append(sql.strip())
    return statements


# Raw tables use the fixture column order (and readable string event IDs).
FIXTURE_TABLES = '''CREATE TABLE benchmark_event_lags (
 region String, cloud_provider String, subscription_id String, data_type String,
 commitment String, event_id String, provider String, event_utc DateTime64(9),
 outcome String, lag_ns Nullable(UInt64), message_bytes Nullable(UInt64)
) ENGINE=Memory;
CREATE TABLE benchmark_subscriber_locations (
 region String, cloud_provider String, latitude Float64,
 longitude Float64, registered_utc DateTime64(9)
) ENGINE=Memory;
CREATE TABLE benchmark_subscription_versions (
 subscription_id String, data_type String, commitment String, filters String,
 registered_utc DateTime64(9)
) ENGINE=Memory;
CREATE TABLE benchmark_connection_events (
 region String, cloud_provider String, subscription_id String, data_type String,
 commitment String, provider String, event_utc DateTime64(9), connected Bool, reason String
) ENGINE=Memory;
'''
ROLLUP_TABLES = r'benchmark_(minute_rollups|dirty_minutes|rollup_commits|dirty_minutes_mv)'
# Dashboards resolve tables via the datasource's default database, whatever its name.
# The summary tables and the dirty-minute view are the production DDL on Memory tables.
SCHEMA = ('CREATE DATABASE any_benchmark_db; USE any_benchmark_db;\n' + FIXTURE_TABLES
          + ''.join(sql + ';\n' for sql in migration_ddl(ROLLUP_TABLES, 'Memory')))


def stamp(seconds, anchor=None):
    """The timestamp `seconds` after the start of the test range."""
    return ((anchor or ANCHOR) + dt.timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S')


def shift(sql):
    """Move fixture timestamps (October 2026) relative to the test range."""
    def moved(match):
        at = dt.datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S').replace(tzinfo=dt.timezone.utc)
        return "'" + stamp((at - FIXTURE_START).total_seconds()) + "'"
    return re.sub(r"'(2026-10-\d\d \d\d:\d\d:\d\d)'", moved, sql)


def render(sql, start=0, end=3600, interval=10, providers=None, anchor=None, **variables):
    """Fill in the Grafana variables and macros the dashboards use."""
    for name, value in variables.items():
        sql = sql.replace('${' + name + ':sqlstring}', repr(value))
    condition = '1' if providers is None else 'provider IN (' + ','.join(map(repr, providers)) + ')'
    sql = sql.replace('$__conditionalAll(provider IN (${provider:sqlstring}), $provider)', condition)
    start, end = stamp(start, anchor), stamp(end, anchor)
    sql = sql.replace('$__timeFilter_ms(event_utc)', f"event_utc >= '{start}' AND event_utc <= '{end}'")
    sql = sql.replace('$__fromTime_ms', f"toDateTime64('{start}', 3, 'UTC')")
    sql = sql.replace('$__toTime_ms', f"toDateTime64('{end}', 3, 'UTC')")
    sql = re.sub(r'\$__timeInterval\((\w+)\)', lambda m: f'toStartOfInterval({m[1]}, INTERVAL {interval} SECOND)', sql)
    sql = sql.replace('$__interval_s', str(interval))
    assert '$' not in sql.replace('$$', ''), re.findall(r'\$\S+', sql)[:3]
    return sql


def clickhouse_local(sql):
    # A fresh data path per run: fixtures must never see another run's databases.
    with tempfile.TemporaryDirectory() as path:
        output = subprocess.check_output(['clickhouse', 'local', '--path', path, '--multiquery',
                                          '--query', sql + ' FORMAT JSONEachRow'],
                                         text=True, stdin=subprocess.DEVNULL)
    return [json.loads(line) for line in output.splitlines() if line]


def run_local(setup, sql, **options):
    """Run a dashboard query against `setup` (fixture rows, 2026-10-08 timestamps)."""
    return clickhouse_local(SCHEMA + shift(setup) + render(sql, **options))
