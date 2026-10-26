"""Multi-stream fixture shared by the dashboard SQL tests.

One subscriber (FRA2 / example-cloud) and providers A and B, with an older and a
newer configuration per stream. Lags are in milliseconds; None means missed.
"""
rows = []


def event(kind, version, key, second, lags, bytes_count, commitment='processed', region='FRA2'):
    for provider, lag in lags.items():
        outcome = 'missed' if lag is None else 'on_time'
        ns = 'NULL' if lag is None else str(lag * 1000000)
        size = 'NULL' if bytes_count is None or lag is None else str(bytes_count)
        rows.append(f"('{region}','example-cloud','{version}','{kind}','{commitment}','{key}',"
                    f"'{provider}','2026-10-08 00:00:{second:02d}','{outcome}',{ns},{size})")


# Reuse event IDs across streams to detect accidental cross-stream joins.
event('transaction', 'tx-old', 'shared-1', 1, {'A': 0, 'B': 900}, 9999)
event('account', 'ac-old', 'shared-1', 2, {'A': 900, 'B': 0}, 9999)
event('slot', 'sl-old', 'shared-1', 3, {'A': 900, 'B': 0}, None)
event('transaction', 'tx-new', 'shared-1', 10, {'A': 0, 'B': 10}, 100)
event('transaction', 'tx-new', 'shared-2', 10, {'A': 20, 'B': 0}, 100)
rows.append(rows[-1])  # A repeated B win must not count twice.
event('account', 'ac-new', 'shared-1', 11, {'A': 0, 'B': 30}, 200)
event('account', 'ac-new', 'shared-2', 11, {'A': 0, 'B': 30}, 200)
event('slot', 'sl-new', 'shared-1', 12, {'A': 40, 'B': 0}, None)
event('slot', 'sl-new', 'shared-2', 12, {'A': 0, 'B': 0}, None)
event('transaction', 'tx-new', 'unpaired', 13, {'A': 0, 'B': None}, 100)
# Newer observations outside the selected location or commitment must not win selection.
event('transaction', 'wrong-commitment', 'shared-1', 30, {'A': 0, 'B': 500}, 9999, commitment='finalized')
event('account', 'wrong-region', 'shared-1', 30, {'A': 0, 'B': 500}, 9999, region='other')

LOCATIONS = "INSERT INTO benchmark_subscriber_locations VALUES ('FRA2','example-cloud',50.1109,8.6821,'2026-10-08 00:00:00');"
versions = [('tx-old', 'transaction'), ('tx-new', 'transaction'), ('ac-old', 'account'),
            ('ac-new', 'account'), ('sl-old', 'slot'), ('sl-new', 'slot')]
SETUP = ('INSERT INTO benchmark_event_lags VALUES ' + ','.join(rows) + ';' + LOCATIONS
         + 'INSERT INTO benchmark_subscription_versions VALUES ' + ','.join(
             f"('{version}','{kind}','processed','[]','2026-10-08 00:00:00')" for version, kind in versions) + ';'
         + 'INSERT INTO benchmark_connection_events VALUES ' + ','.join(
             f"('FRA2','example-cloud','{version}','{kind}','processed','A','2026-10-08 00:00:10',true,'subscribed')"
             for version, kind in versions) + ';')
