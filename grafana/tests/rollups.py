"""Minute summaries against a real ClickHouse server: the routed dashboard queries
must match their exact raw-row forms, through rebuilds, retries, cleanup and
expired raw data.

Needs BENCH_CH_USER and BENCH_CH_PASSWORD (BENCH_CH_URL defaults to
http://127.0.0.1:8123), dashboard dependencies and a Rust toolchain. It never
touches the configured database: it creates, and finally drops, a unique
benchmark_rollup_test_* database.
"""
import base64
import datetime as dt
import json
import math
import os
import re
import subprocess
import tempfile
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request

from common import (ANCHOR, FIXTURE_TABLES, QUERIES, ROLLUP_TABLES, ROOT, build_sql, migration_ddl, render,
                    rollup_sql, shift, stamp, tsx)
from stream_fixture import SETUP

BUILD = build_sql()
PENDING = rollup_sql('pending')
CLEANUP = rollup_sql('cleanup')
EXACT = QUERIES['exactQueries']
SCOPED_COUNTS = tsx("""
import { withRollups, states } from './grafana/rollups.ts';
console.log(JSON.stringify(withRollups("SELECT region,cloud_provider,provider,sum(delivered) delivered FROM ("
  + states("region LIKE 'scope-%'") + ") WHERE kind='provider' GROUP BY region,cloud_provider,provider")));
""")


class ClickHouse:
    def __init__(self):
        self.url = os.getenv('BENCH_CH_URL', 'http://127.0.0.1:8123').rstrip('/')
        self.database = os.getenv('BENCH_CH_DATABASE', 'benchmark')
        self.user = os.environ['BENCH_CH_USER']
        self.password = os.environ['BENCH_CH_PASSWORD']

    def query(self, sql, **params):
        args = {'database': self.database, **{'param_' + k: str(v) for k, v in params.items()}}
        request = urllib.request.Request(self.url + '/?' + urllib.parse.urlencode(args), sql.encode())
        auth = base64.b64encode((self.user + ':' + self.password).encode()).decode()
        request.add_header('Authorization', 'Basic ' + auth)
        try:
            with urllib.request.urlopen(request, timeout=150) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            raise AssertionError(error.read().decode()) from error

    def rows(self, sql, **params):
        return [json.loads(line) for line in self.query(sql + ' FORMAT JSONEachRow', **params).splitlines()]


def pending(client, limit):
    return [row for scope in client.rows('SELECT DISTINCT region,cloud_provider FROM benchmark_dirty_minutes')
            for row in client.rows(PENDING, limit=limit, include_open=1, settle_ms=0, **scope)]


c = ClickHouse()
database = 'benchmark_rollup_test_' + uuid.uuid4().hex
c.query('CREATE DATABASE ' + database)
c.database = database
anchor = ANCHOR  # Moved back later to test expired raw data.


def at(seconds):
    return stamp(seconds, anchor)


def query(sql, seconds=7200, interval=60, start=0, a='A', b='B', providers=None, stream='__all', subscription='__latest'):
    sql = render(sql, start=start, end=seconds, interval=interval, providers=providers, anchor=anchor,
                 region='FRA2', cloud_provider='example-cloud', data_type=stream, commitment='processed',
                 subscription_id=subscription, provider_a=a, provider_b=b)
    try:
        return c.rows(sql)
    except urllib.error.HTTPError as error:
        raise AssertionError(error.read().decode()) from error


def equal(left, right):
    left = sorted(left, key=lambda r: json.dumps(r, sort_keys=True))
    right = sorted(right, key=lambda r: json.dumps(r, sort_keys=True))
    assert len(left) == len(right), (left, right)
    for a, b in zip(left, right):
        assert a.keys() == b.keys(), (a, b)
        for key in a:
            if isinstance(a[key], (int, float)) and isinstance(b[key], (int, float)):
                assert math.isclose(a[key], b[key], rel_tol=1e-9, abs_tol=1e-9), (key, a, b)
            else:
                assert a[key] == b[key], (key, a, b)


def publish_scope(region, cloud, shutdown=False, fixture_database=None):
    result = subprocess.run(['cargo', 'test', '--quiet', '--lib', 'rollups::tests::publish_fixture',
                    '--', '--ignored', '--exact'], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                   env={**os.environ, 'BENCH_CH_DATABASE':fixture_database or database,
                        'ROLLUP_TEST_REGION':region, 'ROLLUP_TEST_CLOUD':cloud,
                        'ROLLUP_TEST_SHUTDOWN':'unclean' if shutdown == 'unclean' else '1' if shutdown else '0'})
    assert result.returncode == 0, result.stdout


def drain():
    for scope in c.rows('SELECT DISTINCT region,cloud_provider FROM benchmark_dirty_minutes'):
        publish_scope(scope['region'], scope['cloud_provider'])
    assert pending(c, 100) == []


try:
    # Fixture raw tables use Memory and short event IDs; the summary tables and the
    # dirty-minute view are the production DDL.
    for sql in FIXTURE_TABLES.split(';') + migration_ddl(ROLLUP_TABLES):
        if sql.strip():
            c.query(sql)
    # Dirty markers must outlive the summaries they invalidate; otherwise an
    # unrebuildable stale summary becomes valid again when its marker expires.
    ttl_days = {row['name']: int(re.search(r'toIntervalDay\((\d+)\)', row['engine_full']).group(1))
                for row in c.rows("SELECT name, engine_full FROM system.tables WHERE database = currentDatabase() "
                                  "AND name IN ('benchmark_dirty_minutes', 'benchmark_minute_rollups', 'benchmark_rollup_commits')")}
    assert ttl_days['benchmark_dirty_minutes'] > max(ttl_days['benchmark_minute_rollups'], ttl_days['benchmark_rollup_commits']), ttl_days
    for sql in shift(SETUP).split(';'):
        if sql.strip():
            c.query(sql)
    configurations = query(QUERIES['subscriptionDetails'])
    drain()
    routed = ['pairSummary', 'pairTrends', 'providerLagSummaries', 'providerMetricsTrend',
              'providerWinSummary', 'providerWinTrend', 'pairDroppedRate', 'locationWinRates']
    # Recent, ten-second route preserves every exact value and field.
    for name in routed:
        equal(query(QUERIES[name], seconds=1800, interval=10), query(EXACT[name], seconds=1800, interval=10))
    # Rolled counts, signed means, wins, throughput, hidden providers, reverse pairs.
    for options in [{}, {'a':'B','b':'A'}, {'providers':['A']}, {'stream':'transaction'},
                    {'stream':'slot'}, {'stream':'transaction','subscription':'tx-old'}, {'a':'A','b':'A'}]:
        for name in routed[:-1]:
            left, right = query(QUERIES[name], **options), query(EXACT[name], **options)
            def counts(rows):
                return [{k:v for k,v in row.items() if not k.startswith(('p0','p1','p5','p9','median_'))} for row in rows]
            equal(counts(left), counts(right))
    # Detail remains available for a six-hour span; longer spans use summaries.
    for key in ['providerMetricsTrend', 'providerWinTrend', 'pairTrends', 'pairDroppedRate']:
        equal(query(QUERIES[key], seconds=21600, interval=10), query(EXACT[key], seconds=21600, interval=10))
        equal(query(QUERIES[key], seconds=21601, interval=10), query(QUERIES[key], seconds=21601, interval=60))
    # Partial start/end boundaries must use raw records and never overlap summaries.
    for start, end in [(11,7200),(0,12),(11,12)]:
        equal(query(QUERIES['providerWinSummary'], start=start, seconds=end), query(EXACT['providerWinSummary'], start=start, seconds=end))
    equal(query(QUERIES['subscriptionDetails']), configurations)
    assert query(QUERIES['locationWinRates'])[0]['win_rates'] == query(EXACT['locationWinRates'])[0]['win_rates']

    def insert(provider, lag, outcome='on_time', event='split', second=70):
        c.query(f"INSERT INTO benchmark_event_lags VALUES ('FRA2','example-cloud','tx-new','transaction','processed','{event}','{provider}','{at(second)}','{outcome}',{lag},100)")
    insert('A', 0)
    insert('A', 0, event='revision-baseline')
    drain()  # Publish a sole arrival: not yet a win.
    insert('B', 5000000)  # Another insert completes the pair in an already published minute.
    # Revisions are cumulative row counts. A one-row increment smaller than the
    # published revision must still invalidate it, without consulting any clock.
    revision = c.rows("SELECT sum(revision) revision FROM benchmark_dirty_minutes "
                      "WHERE region='FRA2' AND bucket={bucket:DateTime}", bucket=at(60))[0]['revision']
    committed_revision = c.rows("SELECT argMax(revision,generation) revision FROM benchmark_rollup_commits "
                                "WHERE region='FRA2' AND bucket={bucket:DateTime}", bucket=at(60))[0]['revision']
    assert revision == committed_revision + 1 and committed_revision > 1, (revision, committed_revision)
    c.query('OPTIMIZE TABLE benchmark_dirty_minutes FINAL')
    assert c.rows("SELECT sum(revision) revision FROM benchmark_dirty_minutes "
                  "WHERE region='FRA2' AND bucket={bucket:DateTime}", bucket=at(60))[0]['revision'] == revision
    for key in ['providerWinSummary', 'pairDroppedRate']:
        equal(query(QUERIES[key]), query(EXACT[key]))  # Dirty minute falls back immediately.
    # Orphan generation simulates failure after rows were written, before publication.
    c.query(BUILD, bucket=at(60), generation=123, region='FRA2', cloud_provider='example-cloud')
    equal(query(QUERIES['providerWinSummary']), query(EXACT['providerWinSummary']))
    drain()
    insert('B', 5000000)  # Ambiguous retry of the exact same observation.
    drain()
    equal(query(QUERIES['providerWinSummary']), query(EXACT['providerWinSummary']))
    equal(query(QUERIES['pairDroppedRate']), query(EXACT['pairDroppedRate']))
    # Cleanup keeps recently superseded generations (a dashboard query may still read
    # them), then deletes superseded and orphaned ones without changing any result.
    def generations():
        return c.rows("SELECT toString(bucket) bucket, arraySort(groupUniqArray(generation)) g FROM benchmark_minute_rollups "
                      "WHERE region='FRA2' GROUP BY bucket ORDER BY bucket")
    published = c.rows("SELECT toString(bucket) bucket, [max(generation)] g FROM benchmark_rollup_commits "
                       "WHERE region='FRA2' GROUP BY bucket ORDER BY bucket")
    before_cleanup, kept = query(QUERIES['providerWinSummary']), generations()
    assert kept != published, 'fixture has superseded and orphaned generations'
    c.query(CLEANUP, region='FRA2', cloud_provider='example-cloud', cutoff=time.time_ns() - 600 * 10**9)
    assert generations() == kept
    c.query(CLEANUP, region='FRA2', cloud_provider='example-cloud', cutoff=time.time_ns() + 3600 * 10**9)
    assert generations() == published, generations()
    equal(query(QUERIES['providerWinSummary']), before_cleanup)
    equal(query(QUERIES['pairDroppedRate']), query(EXACT['pairDroppedRate']))
    # Same minute across both different regions and different clouds: publication
    # and dirty invalidation must remain independent, including map-style unions.
    scopes = [('scope-A','cloud-1'), ('scope-B','cloud-1'), ('scope-A','cloud-2')]
    def scope_insert(region, cloud, event):
        c.query(f"INSERT INTO benchmark_event_lags VALUES ('{region}','{cloud}','s','slot','processed','{event}','A','{at(120)}','on_time',0,NULL)")
    for region, cloud in scopes:
        scope_insert(region, cloud, 'one')
    publish_scope(*scopes[0])
    expected = [{'region':r,'cloud_provider':cl,'provider':'A','delivered':1} for r,cl in scopes]
    equal(query(SCOPED_COUNTS), expected)
    assert len(c.rows("SELECT DISTINCT region,cloud_provider FROM benchmark_rollup_commits WHERE region LIKE 'scope-%'")) == 1
    publish_scope(*scopes[1])
    stable = c.rows("SELECT * FROM benchmark_rollup_commits WHERE region='scope-B'")
    scope_insert(*scopes[0], 'two')
    expected[0]['delivered'] = 2
    equal(query(SCOPED_COUNTS), expected)
    publish_scope(*scopes[0])
    assert c.rows("SELECT * FROM benchmark_rollup_commits WHERE region='scope-B'") == stable
    equal(query(SCOPED_COUNTS), expected)
    drain()
    # Exercise nanosecond timestamp bounds on a real sparse MergeTree index.
    # Memory-table fixtures cannot catch mixed DateTime/DateTime64 pruning errors.
    c.query("CREATE TABLE benchmark_precision_lags AS benchmark_event_lags ENGINE=MergeTree ORDER BY (subscription_id,region,cloud_provider,data_type,commitment,event_utc,event_id,provider,outcome)")
    c.query("CREATE TABLE benchmark_precision_rollups AS benchmark_minute_rollups ENGINE=Memory")
    c.query("""INSERT INTO benchmark_precision_lags
    SELECT 'precision','example-cloud','precision','transaction','processed',toString(intDiv(number,2)),
      if(number%2=0,'A','B'), {start:DateTime64(9, 'UTC')} + toIntervalNanosecond(intDiv(number,2)*1000000+999999),
      'on_time',if(number%2=0,0,5000000),100 FROM numbers(360000)""", start=at(0))
    c.query(BUILD.replace('benchmark_event_lags','benchmark_precision_lags').replace('INSERT INTO benchmark_minute_rollups','INSERT INTO benchmark_precision_rollups'),
            bucket=at(60), generation=1, region='precision', cloud_provider='example-cloud')
    precision = c.rows("SELECT provider,delivered FROM benchmark_precision_rollups WHERE kind='pair' ORDER BY provider")
    assert precision == [{'provider':'A','delivered':60000},{'provider':'B','delivered':60000}], precision
    # Graceful shutdown uses the production run loop and publishes the open
    # minute, even though the normal polling pass deliberately skips it.
    c.query("INSERT INTO benchmark_event_lags VALUES ('shutdown','cloud','s','slot','processed','open','A',now64(9),'on_time',0,NULL)")
    assert not c.rows(PENDING, region='shutdown', cloud_provider='cloud', include_open=0, settle_ms=0, limit=60)
    # A closed minute waits for the settle delay (window close + flush) before its
    # first build, so the rows of its last seconds are not missed and rebuilt.
    c.query("INSERT INTO benchmark_event_lags VALUES ('settle','cloud','s','slot','processed','closed','A',now64(9) - INTERVAL 61 SECOND,'on_time',0,NULL)")
    assert c.rows(PENDING, region='settle', cloud_provider='cloud', include_open=0, settle_ms=0, limit=60)
    assert not c.rows(PENDING, region='settle', cloud_provider='cloud', include_open=0, settle_ms=120000, limit=60)
    publish_scope('settle', 'cloud')
    publish_scope('shutdown', 'cloud', shutdown=True)
    assert len(c.rows("SELECT * FROM benchmark_rollup_commits WHERE region='shutdown'")) == 1
    assert not c.rows(PENDING, region='shutdown', cloud_provider='cloud', include_open=1, settle_ms=0, limit=60)

    # Real Rust writer, real production types, and a rejected final insert. The
    # acknowledged intent must survive shutdown/restart, hide old summaries AND
    # raw fallback, and override earlier healthy evidence for the failed minute.
    failed_database = database + '_failed_ingestion'
    c.query('CREATE DATABASE ' + failed_database)
    failed = ClickHouse()
    failed.database = failed_database
    journal_directory = tempfile.TemporaryDirectory(prefix='benchmark-write-journal-')
    def write_failure(recover=False, intent=False):
        subprocess.run(['cargo','test','--quiet','--lib','storage::tests::failed_final_batch_fixture',
                        '--','--ignored','--exact'], cwd=ROOT, check=True,
                       env={**os.environ,'BENCH_CH_DATABASE':failed_database,'BENCH_TEST_STATE_DIR':journal_directory.name,
                            'BENCH_TEST_RECOVER':'1' if recover else '0', 'BENCH_TEST_FAIL_INTENT':'1' if intent else '0'})
    try:
        for sql in migration_ddl(r'benchmark_.*'):
            failed.query(sql)
        failed.query("ALTER TABLE benchmark_event_lags ADD CONSTRAINT reject_missed CHECK outcome != 'missed'")
        write_failure()
        minute = failed.rows("SELECT toString(bucket) bucket,sum(revision) revision,sum(expected) expected "
                             "FROM benchmark_dirty_minutes GROUP BY bucket")[0]
        assert (minute['revision'], minute['expected']) == (1,2), minute
        assert failed.rows('SELECT count() n FROM benchmark_event_lags')[0]['n'] == 1
        write_failure(recover=True)
        assert failed.rows('SELECT sum(incomplete) n FROM benchmark_dirty_minutes')[0]['n'] == 1
        for shutdown in ['unclean', False, True]:
            publish_scope('us-east','example-cloud',shutdown=shutdown,fixture_database=failed_database)
            assert failed.rows('SELECT count() n FROM benchmark_rollup_commits')[0]['n'] == 0
        # Simulate a previously published generation: the new unmatched intent
        # must exclude it even when the actual raw revision has not increased.
        failed.query(BUILD,bucket=minute['bucket'],generation=1,region='us-east',cloud_provider='example-cloud')
        failed.query("INSERT INTO benchmark_rollup_commits VALUES ('us-east','example-cloud', {bucket:DateTime},1,1)",bucket=minute['bucket'])
        from_time = dt.datetime.strptime(minute['bucket'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=dt.timezone.utc)
        gate = (ROOT / 'sql/reports.sql').read_text().split('-- SHORT-RANGE COVERAGE GATE:')[1]
        gate = 'SELECT if(' + gate.split('SELECT if(',1)[1].split(';')[0]
        coverage = failed.rows(gate,region='us-east',cloud_provider='example-cloud',
                               subscription_id='id-4',provider='b',from_utc=stamp(1,from_time),to_utc=stamp(5,from_time))
        assert coverage == [{'full_range_coverage':'unknown'}], coverage
        for key, seconds, interval in [('providerWinSummary',60,10),('providerWinSummary',7200,60),
                                       ('providerMetricsTrend',60,10),('providerMetricsTrend',60,60),('overview',60,60)]:
            rows = failed.rows(render(QUERIES[key],start=0,end=seconds,interval=interval,anchor=from_time,
                                      region='us-east',cloud_provider='example-cloud',data_type='slot',
                                      commitment='processed',subscription_id='id-4',provider_a='a',provider_b='b'))
            assert rows == [], (key,rows)
        # An intent itself can fail before the database knows which minute was
        # lost. Recovery must still mark that minute unknown from the local file.
        for table in ['benchmark_event_lags','benchmark_dirty_minutes','benchmark_rollup_commits',
                      'benchmark_minute_rollups','benchmark_measurement_health']:
            failed.query('TRUNCATE TABLE ' + table)
        failed.query('ALTER TABLE benchmark_dirty_minutes ADD CONSTRAINT reject_intents CHECK expected=0')
        write_failure(intent=True)
        assert failed.rows('SELECT count() n FROM benchmark_dirty_minutes')[0]['n'] == 0
        assert failed.rows('SELECT count() n FROM benchmark_event_lags')[0]['n'] == 0
        write_failure(recover=True)
        assert failed.rows('SELECT sum(incomplete) n FROM benchmark_dirty_minutes')[0]['n'] == 1
        publish_scope('us-east','example-cloud',shutdown=True,fixture_database=failed_database)
        assert failed.rows('SELECT count() n FROM benchmark_rollup_commits')[0]['n'] == 0
    finally:
        journal_directory.cleanup()
        c.query('DROP DATABASE ' + failed_database)
    # A provider disconnected for the whole range has no summaries, only downtime:
    # the Overview must still list it, with blank metrics, next to a provider
    # without disconnects (zero downtime).
    c.query(f"""INSERT INTO benchmark_connection_events VALUES
     ('overview','cloud','ov','transaction','processed','A','{at(0)}',true,'subscribed'),
     ('overview','cloud','ov','transaction','processed','B','{at(0)}',true,'subscribed'),
     ('overview','cloud','ov','transaction','processed','B','{at(300)}',false,'provider error')""")
    c.query("INSERT INTO benchmark_event_lags VALUES " + ','.join(
        f"('overview','cloud','ov','transaction','processed','e{n}','A','{at(310 + n)}','on_time',0,100)"
        for n in range(3)))
    drain()
    overview = [row for row in query(QUERIES['overview'], start=300, seconds=360)
                if row['location'] == 'overview']
    blank = dict(win_percent=None, p50_lag_ms=None, p99_lag_ms=None, dropped_percent=None, win_cell=None)
    equal(overview, [
        {'location':'overview', 'provider':'A', **blank, 'p50_lag_ms':0, 'p99_lag_ms':0, 'dropped_percent':0,
         'disconnected_seconds':0, 'disconnects':0},
        {'location':'overview', 'provider':'B', **blank, 'disconnected_seconds':60, 'disconnects':1},
    ])
    # Completed summaries remain queryable after raw history is removed.
    before = query(QUERIES['providerWinSummary'])
    c.query('TRUNCATE TABLE benchmark_event_lags')
    equal(query(QUERIES['providerWinSummary']), before)
    # Historical short windows automatically use minute resolution after raw TTL.
    c.query("INSERT INTO benchmark_minute_rollups SELECT * REPLACE(bucket - INTERVAL 3 DAY AS bucket, first_seen - INTERVAL 3 DAY AS first_seen, last_seen - INTERVAL 3 DAY AS last_seen) FROM benchmark_minute_rollups")
    c.query("INSERT INTO benchmark_rollup_commits SELECT region, cloud_provider, bucket - INTERVAL 3 DAY, generation, revision FROM benchmark_rollup_commits")
    anchor -= dt.timedelta(days=3)
    equal(query(QUERIES['providerWinSummary'], seconds=1800, interval=10), before)
    historical_rates = query(QUERIES['providerMetricsTrend'], seconds=1800, interval=10)
    assert historical_rates and all(row['events_per_second'] < 1 for row in historical_rates)
    # Raw rows are gone: partial edge minutes use whole-minute summaries instead of
    # disappearing. A sub-minute range shows its whole minute; ragged edges widen.
    whole_minute = query(QUERIES['providerWinSummary'], start=0, seconds=60)
    assert whole_minute, 'fixture minute has wins'
    equal(query(QUERIES['providerWinSummary'], start=5, seconds=20), whole_minute)
    equal(query(QUERIES['providerWinSummary'], start=30, seconds=90),
          query(QUERIES['providerWinSummary'], start=0, seconds=120))
    equal(query(QUERIES['providerMetricsTrend'], start=30, seconds=90, interval=60),
          query(QUERIES['providerMetricsTrend'], start=0, seconds=120, interval=60))
    print('Rollup integration passed: raw/summary routes, exact counters, reverse pairs, All/history/hidden providers, partial boundaries, split inserts, cumulative revisions through merges, retries, unpublished generations, generation cleanup, retained summaries, subscriber isolation, shutdown of an open minute, rejected final inserts and intents with restart recovery, coverage gating, and Overview rows for providers that were down throughout.')
finally:
    c.query('DROP DATABASE ' + database)
