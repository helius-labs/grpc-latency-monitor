"""Provider win rates by subscriber location (the map query).

Run with: python3 grafana/tests/location_map.py (needs dashboard dependencies and
the clickhouse CLI).
"""
import math

from common import QUERIES, run_local

rows = []


def event(region, cloud, version, key, seconds, arrivals,
          data_type='transaction', commitment='processed', date='2026-10-08'):
    for provider, lag in arrivals.items():
        outcome = 'missed' if lag is None else 'on_time'
        ns = 'NULL' if lag is None else str(lag * 1000000)
        rows.append(f"('{region}','{cloud}','{version}','{data_type}','{commitment}',"
                    f"'{key}','{provider}','{date} 00:00:{seconds:02d}','{outcome}',{ns},NULL)")


for region, cloud, version, winner in [
    ('frankfurt', 'example-cloud', 'fra-current', 'A'),
    ('new-york', 'AWS', 'ny-current', 'B'),
]:
    other = 'B' if winner == 'A' else 'A'
    for key, seconds in [('shared-1', 10), ('shared-2', 11)]:
        event(region, cloud, version, key, seconds, {winner: 0, other: 5})
    event(region, cloud, version, 'shared-3', 12, {winner: 5, other: 0})
    event(region, cloud, version, 'tie', 13, {'A': 0, 'B': 0})
    # Uncontested (other provider not connected, no row): not a win.
    event(region, cloud, version, 'sole', 14, {'A': 0})
    # The other provider dropped it while connected: a win for the deliverer.
    event(region, cloud, version, 'dropped', 15, {winner: 0, other: None})
rows.append(rows[0])  # Ambiguous retry must not create another win or tie.
# Earlier configuration loses even if it would favor another provider.
event('frankfurt', 'example-cloud', 'fra-old', 'old', 1, {'A': 5, 'B': 0})
# Same city, another hosting provider: one independent marker.
event('frankfurt', 'AWS', 'aws-current', 'shared-1', 15, {'A': 5, 'B': 0, 'C': None})
# A zero coordinate is valid, and ties produce an explicit no-wins marker.
event('origin', 'Test', 'origin-current', 'shared-1', 15, {'A': 0, 'B': 0})
# Existing observations without registered metadata do not produce a marker.
event('unregistered', 'Test', 'unknown', 'shared-1', 15, {'A': 0, 'B': 5})
# Off-scope observations must not affect latest selection or winning providers.
event('frankfurt', 'example-cloud', 'off-stream', 'off', 30, {'X': 0, 'Y': 5}, data_type='slot')
event('frankfurt', 'example-cloud', 'off-commitment', 'off', 30, {'X': 0, 'Y': 5}, commitment='finalized')
event('frankfurt', 'example-cloud', 'out-of-range', 'off', 30, {'X': 0, 'Y': 5}, date='2026-10-09')
metadata = '''INSERT INTO benchmark_subscriber_locations VALUES
 ('frankfurt','example-cloud',10,20,'2026-10-07 00:00:00'),
 ('frankfurt','example-cloud',50.1109,8.6821,'2026-10-08 00:00:00'),
 ('frankfurt','AWS',50.1109,8.6821,'2026-10-08 00:00:00'),
 ('new-york','AWS',40.7128,-74.006,'2026-10-08 00:00:00'),
 ('origin','Test',0,0,'2026-10-08 00:00:00');
'''


def run(setup):
    return run_local(setup, QUERIES['locationWinRates'], data_type='transaction', commitment='processed')


result = run('INSERT INTO benchmark_event_lags VALUES ' + ','.join(rows) + ';' + metadata)
markers = {(row['region'], row['cloud_provider']): row for row in result}
assert len(markers) == 4, result
fra = markers[('frankfurt', 'example-cloud')]
ny = markers[('new-york', 'AWS')]
aws = markers[('frankfurt', 'AWS')]
assert fra['total_wins'] == ny['total_wins'] == 4
assert fra['leader'] == 'A' and ny['leader'] == 'B'
assert fra['win_rates'] == 'A: 75% · B: 25%', fra
assert ny['win_rates'] == 'A: 25% · B: 75%', ny
assert aws['win_rates'] == 'A: 0% · B: 100% · C: 0%', aws
assert fra['provider_p50'] == 'A: 0 ms · B: 5 ms', fra
assert fra['provider_p95'] == 'A: 5 ms · B: 5 ms', fra
assert ny['provider_p50'] == 'A: 0 ms · B: 0 ms', ny
assert aws['provider_p50'] == 'A: 5 ms · B: 0 ms · C: No data', aws
assert aws['provider_p95'] == 'A: 5 ms · B: 0 ms · C: No data', aws
assert ny['leader_color'] == aws['leader_color']
assert fra['latitude'] == 50.1109 and fra['longitude'] == 8.6821
assert fra['marker_longitude'] != aws['marker_longitude']
assert math.isclose(ny['marker_longitude'], ny['longitude'])
origin = markers[('origin', 'Test')]
assert origin['latitude'] == origin['longitude'] == 0
assert origin['leader'] == 'No wins' and origin['leader_color'] == 0
assert origin['win_rates'] == 'A: No wins · B: No wins'
assert run('') == []
print('Map fixtures passed: per-location wins, configuration selection, stream/time scope, retries, ties, sole deliveries, zero shares, unregistered locations, valid zero coordinates, stable colors, and separated colocated markers.')
