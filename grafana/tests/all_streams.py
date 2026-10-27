"""All Providers and Head to Head queries: latest-configuration selection, pooled
streams, wins, missed deadlines, downtime and configuration labels.

Run with: python3 grafana/tests/all_streams.py (needs dashboard dependencies and
the clickhouse CLI).
"""
import math

from common import QUERIES, run_local
from stream_fixture import SETUP


def run(key, stream='__all', subscription='__latest', setup=SETUP, provider_a='A', provider_b='B', commitment='processed', **options):
    return run_local(setup, QUERIES[key], region='FRA2', cloud_provider='example-cloud', data_type=stream,
                     commitment=commitment, subscription_id=subscription,
                     provider_a=provider_a, provider_b=provider_b, **options)


def values(key, field, **options):
    return {row['provider']: row[field] for row in run(key, **options)}


def close(actual, expected):
    assert actual.keys() == expected.keys() and all(math.isclose(actual[k], expected[k]) for k in expected), actual


# In All, each stream uses its latest configuration: tx-new, ac-new and sl-new.
assert values('providerLagSummaries', 'p50_lag_ms') == {'A': 0, 'B': 10}
assert values('providerLagSummaries', 'p99_lag_ms') == {'A': 40, 'B': 30}
# A wins 3 head-to-head races plus the 'unpaired' event B missed the deadline for; B wins 2.
close(values('providerWinSummary', 'win_percent'), {'A': 200/3, 'B': 100/3})
close(values('providerWinTrend', 'win_percent'), {'A': 200/3, 'B': 100/3})
assert values('providerMetricsTrend', 'events_per_second') == {'A': 0.7, 'B': 0.6}
assert values('providerMetricsTrend', 'bytes_per_second') == {'A': 70, 'B': 60}
assert values('providerMetricsTrend', 'dropped_per_second') == {'A': 0, 'B': 0.1}
assert {row['series']: row['events_per_second'] for row in run('pairDroppedRate')} == {'A': 0, 'B': 0.1}
annotations = run('connectedEvents')
assert len(annotations) == 3 and all('/' in row['text'] for row in annotations), annotations
assert run('disconnectedEvents') == []

# Same-event differences A − B: [-10, 20, -30, -30, 40, 0] ms.
summary = run('pairSummary')[0]
assert math.isclose(summary['avg_ms'], -10/6)
assert (summary['p001_ms'], summary['p1_ms'], summary['p5_ms']) == (-30, -30, -30), summary
assert (summary['p50_ms'], summary['p95_ms'], summary['p99_ms'], summary['p999_ms']) == (0, 40, 40, 40), summary
assert math.isclose(summary['a_win_percent'], 200/3) and math.isclose(summary['b_win_percent'], 100/3)
reverse = run('pairSummary', provider_a='B', provider_b='A')[0]
assert math.isclose(reverse['avg_ms'], 10/6)
assert (reverse['p001_ms'], reverse['p50_ms'], reverse['p95_ms'], reverse['p999_ms']) == (-40, 10, 30, 30), reverse
assert all(value is None for value in run('pairSummary', provider_b='A')[0].values())

# Per stream, and a historical configuration of one stream. History is ignored in All.
for stream, subscription, avg, p50, a_wins, lags in [
    ('transaction', '__latest', 5, 20, 200/3, {'A': 0, 'B': 10}),
    ('account', '__latest', -30, -30, 100, {'A': 0, 'B': 30}),
    ('slot', '__latest', 20, 40, 0, {'A': 40, 'B': 0}),
    ('transaction', 'tx-old', -900, -900, 100, {'A': 0, 'B': 900}),
    ('__all', 'tx-old', -10/6, 0, 200/3, {'A': 0, 'B': 10}),
]:
    options = dict(stream=stream, subscription=subscription)
    summary = run('pairSummary', **options)[0]
    assert math.isclose(summary['avg_ms'], avg) and summary['p50_ms'] == p50, (options, summary)
    assert math.isclose(summary['a_win_percent'], a_wins) and math.isclose(summary['a_win_percent'] + summary['b_win_percent'], 100)
    assert values('providerLagSummaries', 'p50_lag_ms', **options) == lags, options
    rows = run('providerMetricsTrend', **options) + run('providerLagSummaries', **options) + run('pairTrends', **options)
    assert all('percentiles' not in row for row in rows)
assert all(row['bytes_per_second'] is None for row in run('providerMetricsTrend', stream='slot'))

# Buckets keep their own populations when streams land in different buckets.
shifted = SETUP.replace('00:00:11', '00:00:21').replace('00:00:12', '00:00:32')
trends = [(row['avg_a_minus_b_ms'], row['median_a_minus_b_ms'], round(row['a_win_percent'], 2), round(row['b_win_percent'], 2))
          for row in run('pairTrends', setup=shifted)]
assert trends == [(5, 20, 66.67, 33.33), (-30, -30, 100, 0), (20, 40, 0, 100)], trends
metrics = [(row['provider'], row['p50_lag_ms'], row['events_per_second'], row['dropped_per_second'], row['bytes_per_second'])
           for row in run('providerMetricsTrend', setup=shifted)]
assert metrics == [('A', 0, 0.3, 0, 30), ('B', 10, 0.2, 0.1, 20), ('A', 0, 0.2, 0, 40),
                   ('B', 30, 0.2, 0, 40), ('A', 40, 0.2, 0, None), ('B', 0, 0.2, 0, None)], metrics

marker = run('locationWinRates')
assert len(marker) == 1, marker
assert marker[0]['win_rates'] == 'A: 66.67% · B: 33.33%', marker
assert marker[0]['provider_p50'] == 'A: 0 ms · B: 10 ms', marker
assert marker[0]['provider_p95'] == 'A: 40 ms · B: 30 ms', marker

assert run('subscriptionOptions') == []
assert len(run('subscriptionOptions', stream='transaction')) == 2
configs = run('subscriptionDetails')
assert {row['stream'] for row in configs} == {'account', 'transaction', 'slot'}
assert {row['configuration'] for row in configs} == {'Account owner: all', 'Transactions include: all', 'Slot updates (all statuses)'}
# The on-time window and the compared providers are part of a configuration's
# identity and label; endpoints are only stored as hashes and never shown.
windowed = SETUP.replace("('tx-new','transaction','processed','[]'",
    "('tx-new','transaction','processed','transaction;commitment=processed;window=5s;providers=A@1a2b3c4d,B@5e6f7a8b;transactions.account_include=[]'")
assert 'Transactions include: all · window 5s · A, B' in {row['configuration'] for row in run('subscriptionDetails', setup=windowed)}

# Transaction-aware account histories are visibly distinct from the old
# first-update-per-account-and-slot histories; older labels remain unchanged.
transaction_accounts = SETUP.replace("('ac-new','account','processed','[]'",
    "('ac-new','account','processed','account;commitment=processed;window=5s;accounts.owner=[];nonempty_txn_signature=true;account_key=slot,pubkey,txn_signature'")
account_labels = {row['configuration'] for row in run('subscriptionDetails', setup=transaction_accounts) if row['stream'] == 'account'}
assert account_labels == {'Account owner: all', 'Account owner: all · per transaction · window 5s'}, account_labels

# Slots have one all-status stream, stored under processed. Both commitment views
# use it, and retired finalized-labelled duplicates cannot change metrics or health.
duplicate_slots = SETUP + """
INSERT INTO benchmark_event_lags VALUES
 ('FRA2','example-cloud','sl-duplicate','slot','finalized','duplicate','A','2026-10-08 00:00:20','on_time',0,NULL),
 ('FRA2','example-cloud','sl-duplicate','slot','finalized','duplicate','B','2026-10-08 00:00:20','missed',NULL,NULL);
INSERT INTO benchmark_subscription_versions VALUES
 ('sl-duplicate','slot','finalized','[]','2026-10-08 00:00:00');
INSERT INTO benchmark_connection_events VALUES
 ('FRA2','example-cloud','sl-duplicate','slot','finalized','A','2026-10-08 00:00:10',false,'stream EOF'),
 ('FRA2','example-cloud','sl-duplicate','slot','finalized','A','2026-10-08 00:00:30',true,'subscribed');
"""
for key in ['providerMetricsTrend', 'providerWinSummary', 'pairSummary', 'subscriptionDetails', 'providerDowntime']:
    expected = run(key, stream='slot')
    assert expected, key
    assert run(key, stream='slot', commitment='finalized', setup=duplicate_slots) == expected, key
for key in ['providerMetricsTrend', 'pairSummary', 'locationWinRates', 'providerDowntime']:
    assert run(key, commitment='finalized', setup=duplicate_slots) == run(key, commitment='finalized'), key

# Time ranges without observations produce no selected subscription or false wins.
assert run('providerLagSummaries', setup='') == run('providerWinSummary', setup='') == []
assert run('locationWinRates', setup='') == run('providerMetricsTrend', setup='') == []
assert all(value is None for value in run('pairSummary', setup='')[0].values())
unknown_sizes = SETUP.replace("100)", "NULL)").replace("200)", "NULL)")
assert all(row['bytes_per_second'] is None for row in run('providerMetricsTrend', setup=unknown_sizes))

# Hidden providers still compete: a displayed provider can lose to a hidden winner.
assert values('providerWinSummary', 'win_percent', providers=['A']) == {'A': 100}
assert values('providerWinSummary', 'win_percent', providers=['B']) == {'B': 100}
hidden_winner = SETUP + "INSERT INTO benchmark_event_lags VALUES " + ','.join(
    f"('FRA2','example-cloud','tx-new','transaction','processed','hidden-winner','{provider}','2026-10-08 00:00:14','on_time',{lag},100)"
    for provider, lag in [('A', 5000000), ('B', 10000000), ('C', 0)]) + ';'
close(values('providerWinSummary', 'win_percent', providers=['A', 'B'], setup=hidden_winner), {'A': 200/3, 'B': 100/3})
assert values('providerWinSummary', 'win_percent', providers=['C'], setup=hidden_winner) == {'C': 100}

# Without a competitor (B not connected, no row), a sole delivery is not a win.
uncontested = SETUP.replace("'unpaired','B','2026-10-08 00:00:13','missed'", "'unpaired','B','2026-10-08 00:00:13','ignored'")
assert uncontested != SETUP
assert values('providerWinSummary', 'win_percent', setup=uncontested) == {'A': 60, 'B': 40}
# A miss in a slot that never confirmed is `forked`: neither a missed deadline nor a competitor.
forked = SETUP.replace("'unpaired','B','2026-10-08 00:00:13','missed'", "'unpaired','B','2026-10-08 00:00:13','forked'")
assert values('providerWinSummary', 'win_percent', setup=forked) == {'A': 60, 'B': 40}
assert values('providerMetricsTrend', 'dropped_per_second', setup=forked) == {'A': 0, 'B': 0}

# Downtime counts outages of at least 10 s, in full. B's transaction stream
# disconnects at :20, fails a retry at :25 and resubscribes at :50 (30 s). Its slot
# stream disconnects 60 s before the range ends and never returns; a disconnect
# from the previous day ends inside the range.
transitions = [('tx-new', 'transaction', 'B', '2026-10-08 00:00:05', True), ('tx-new', 'transaction', 'B', '2026-10-08 00:00:20', False),
               ('tx-new', 'transaction', 'B', '2026-10-08 00:00:25', False), ('tx-new', 'transaction', 'B', '2026-10-08 00:00:50', True),
               ('sl-new', 'slot', 'B', '2026-10-08 00:59:00', False),
               ('ac-new', 'account', 'B', '2026-10-07 23:59:00', False), ('ac-new', 'account', 'B', '2026-10-08 00:00:30', True)]
# A rolling restart B recovers from in 3 s is a disconnect, not downtime. An outage
# 5 s before the range end that lasts past the range end plus 10 s counts its 5 s
# in range; A's 5 s reconnect across the range end does not.
transitions += [('ac-new', 'account', 'B', '2026-10-08 00:10:00', False), ('ac-new', 'account', 'B', '2026-10-08 00:10:03', True),
                ('ac-new', 'account', 'B', '2026-10-08 00:59:55', False), ('ac-new', 'account', 'B', '2026-10-08 01:00:20', True),
                ('ac-new', 'account', 'A', '2026-10-08 00:59:58', False), ('ac-new', 'account', 'A', '2026-10-08 01:00:03', True)]
# A's own backlog forces a reconnect (not provider downtime); a provider failure on
# the first reconnect attempt is a disconnect, but at 2 s (:42 to :44) not downtime.
transitions += [('tx-new', 'transaction', 'A', '2026-10-08 00:00:40', False, 'arrival queue full'),
                ('tx-new', 'transaction', 'A', '2026-10-08 00:00:42', False, 'connection failed'),
                ('tx-new', 'transaction', 'A', '2026-10-08 00:00:44', True, 'subscribed'),
                ('ac-new', 'account', 'A', '2026-10-08 00:00:40', False, 'arrival queue full'),
                ('ac-new', 'account', 'A', '2026-10-08 00:00:41', True, 'subscribed')]
# A resubscription dropped because the matcher could not take its connection yet is
# also A's own backlog: the 39 s until the next subscription is not downtime.
transitions += [('sl-new', 'slot', 'A', '2026-10-08 00:30:00', False, 'arrival queue full'),
                ('sl-new', 'slot', 'A', '2026-10-08 00:30:01', False, 'control queue full'),
                ('sl-new', 'slot', 'A', '2026-10-08 00:30:40', True, 'subscribed')]
outage = SETUP + 'INSERT INTO benchmark_connection_events VALUES ' + ','.join(
    f"('FRA2','example-cloud','{t[0]}','{t[1]}','processed','{t[2]}','{t[3]}',{str(t[4]).lower()},'{t[5] if len(t) > 5 else 'test'}')"
    for t in transitions) + ';'
downtime = {row['provider']: row for row in run('providerDowntime', setup=outage)}
assert downtime['A']['disconnected_seconds'] == 0 and downtime['A']['disconnects'] == 2, downtime
assert downtime['B']['disconnected_seconds'] == 30 + 60 + 30 + 5, downtime
assert downtime['B']['disconnects'] == 4, downtime  # the retry and the earlier outage don't recount
assert values('providerDowntime', 'disconnected_seconds', stream='transaction', setup=outage) == {'A': 0, 'B': 30}
assert values('providerDowntime', 'disconnected_seconds', stream='account', setup=outage) == {'A': 0, 'B': 35}
pair = run('pairDowntime', setup=outage)[0]
assert (pair['a_disconnected_seconds'], pair['b_disconnected_seconds']) == (0, 125), pair
assert (pair['a_disconnects'], pair['b_disconnects']) == (2, 4), pair
assert len(run('disconnectedEvents', setup=outage)) > 0

print('All-stream fixtures passed: latest configurations, pooled percentiles and wins, safe event joins, '
      'rates, throughput, missed deadlines, downtime, annotations, history, map pooling, labels and empty ranges.')
