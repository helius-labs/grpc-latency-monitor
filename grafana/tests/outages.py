"""Ranges with outages but no delivered events.

Run with: python3 grafana/tests/outages.py (needs dashboard dependencies and the
clickhouse CLI).
"""
from common import QUERIES, run_local
from stream_fixture import SETUP


def run(key, stream='__all', subscription='__latest', setup=SETUP):
    return run_local(setup, QUERIES[key], region='FRA2', cloud_provider='example-cloud', data_type=stream,
                     commitment='processed', subscription_id=subscription, provider_a='A', provider_b='B')


# The outage starts before the range, with no retry or transition inside it.
rows = [('FRA2', 'example-cloud', 'outage', 'transaction', 'processed', p, '2026-10-07 23:59:00') for p in ['A', 'B']]
# Future, stale, other-location and other-commitment histories cannot take over.
rows += [
    ('FRA2', 'example-cloud', 'future', 'transaction', 'processed', 'C', '2026-10-08 01:00:01'),
    ('FRA2', 'example-cloud', 'stale', 'transaction', 'processed', 'D', '2026-10-06 00:00:00'),
    ('elsewhere', 'example-cloud', 'other', 'transaction', 'processed', 'E', '2026-10-08 00:00:01'),
    ('FRA2', 'example-cloud', 'final', 'transaction', 'finalized', 'F', '2026-10-08 00:00:01'),
]
outage = 'INSERT INTO benchmark_connection_events VALUES ' + ','.join(
    '(' + ','.join(map(repr, row)) + ",false,'connection failed')" for row in rows) + ';'
for stream in ['transaction', '__all']:
    result = run('pairDowntime', stream=stream, setup=outage)[0]
    assert result == dict(a_disconnected_seconds=3600, b_disconnected_seconds=3600,
                          a_disconnects=0, b_disconnects=0), (stream, result)
    downtime = {row['provider']: row['disconnected_seconds'] for row in run('providerDowntime', stream=stream, setup=outage)}
    assert downtime == {'A': 3600, 'B': 3600}, (stream, downtime)
    assert run('providerOptions', stream=stream, setup=outage) == [{'provider': 'A'}, {'provider': 'B'}]
    overview = [row for row in run('overview', stream=stream, setup=outage) if row['location'] == 'FRA2']
    assert len(overview) == 2, overview
    for row in overview:
        assert row['disconnected_seconds'] == 3600 and row['disconnects'] == 0, row
        assert all(row[field] is None for field in ['win_percent', 'p50_lag_ms', 'p99_lag_ms', 'dropped_percent']), row
# Explicit history selection remains authoritative.
assert run('pairDowntime', stream='transaction', subscription='unseen', setup=outage)[0]['a_disconnected_seconds'] == 0
# New connection history must not replace a configuration with observed events.
observed = SETUP + "INSERT INTO benchmark_connection_events VALUES ('FRA2','example-cloud','new-attempt','transaction','processed','A','2026-10-08 00:30:00',false,'connection failed');"
assert run('pairSummary', stream='transaction', setup=observed) == run('pairSummary', stream='transaction')
print('Outage fixtures passed: no events, prior-range disconnects, latest and All selection, provider options, '
      'Overview, scope isolation and explicit history.')
