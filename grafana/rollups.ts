import { readFileSync } from 'node:fs';

// Committed minutes fully inside the range are eligible. A partial minute at
// either edge uses exact raw rows while they are retained; once its raw rows have
// expired (it starts before the six-hour raw horizon), its whole-minute summary is
// used, widening an old range to whole minutes instead of silently dropping the
// edge. Dirty/unbuilt minutes use raw data; the union never counts a minute twice.
// A scalar subquery is evaluated once per query and reused by every branch.
// This freezes the committed generation list while the worker publishes updates.
const eligibleBucket = `bucket + INTERVAL 1 MINUTE > $__fromTime_ms AND bucket < $__toTime_ms
  AND ((bucket >= $__fromTime_ms AND bucket + INTERVAL 1 MINUTE <= $__toTime_ms) OR bucket < now() - INTERVAL 6 HOUR)`;
const manifest = `SELECT c.region, c.cloud_provider, c.bucket, c.generation FROM
 (SELECT region, cloud_provider, bucket, max(commits.generation) generation, argMax(commits.revision, commits.generation) revision
  FROM benchmark_rollup_commits AS commits
  WHERE ${eligibleBucket} GROUP BY region, cloud_provider, bucket) c
 LEFT JOIN (SELECT region, cloud_provider, bucket, max(revision) revision FROM benchmark_dirty_minutes
  WHERE ${eligibleBucket} GROUP BY region, cloud_provider, bucket) d
 ON c.region = d.region AND c.cloud_provider = d.cloud_provider AND c.bucket = d.bucket WHERE c.revision >= d.revision`;
export function withRollups(sql: string): string {
  return `WITH (SELECT groupArray(tuple(region, cloud_provider, bucket, generation)) FROM (${manifest})) AS rollup_generations
 SELECT * FROM (${sql})`;
}
export const committedMinutes = `SELECT tupleElement(r, 1), tupleElement(r, 2), tupleElement(r, 3), tupleElement(r, 4)
 FROM (SELECT arrayJoin(rollup_generations) AS r)`;
const committedBuckets = `SELECT tupleElement(r, 1), tupleElement(r, 2), tupleElement(r, 3) FROM (SELECT arrayJoin(rollup_generations) AS r)`;
// A positive minute set lets the sparse index prune historical raw granules.
// Bound the fallback grid to raw retention, even for a month-long dashboard.
const rawStart = "toUInt32(toStartOfMinute(greatest($__fromTime_ms, now() - INTERVAL 6 HOUR)))";
const rawEnd = 'toUInt32(toStartOfMinute($__toTime_ms)) + 60';
// Build a positive set of uncommitted subscriber/minute keys. A plain NOT IN
// over raw rows would scan already summarized history before discarding it.
export const rawMinutes = `(region, cloud_provider, toStartOfMinute(event_utc)) IN (
 SELECT region, cloud_provider, toDateTime(second, 'UTC') AS bucket
 FROM (SELECT DISTINCT region, cloud_provider FROM benchmark_dirty_minutes
       UNION DISTINCT SELECT region, cloud_provider FROM benchmark_subscriber_locations
       UNION DISTINCT SELECT tupleElement(r, 1), tupleElement(r, 2)
       FROM (SELECT arrayJoin(rollup_generations) AS r))
 ARRAY JOIN range(${rawStart}, greatest(${rawStart}, ${rawEnd}), 60) AS second
 WHERE (region, cloud_provider, bucket) NOT IN (${committedBuckets}))`;

// The subscriber's rollup worker builds summaries from the same template, so the
// raw fallback counts events exactly like the summaries it stands in for.
const build = readFileSync(new URL('../sql/rollups/build.sql', import.meta.url), 'utf8')
  .replace(/^(--.*\n)+/, '').trimEnd();
export function rawStates(filter: string): string {
  return build
    .replace('{{source_filter}}', () => `(${filter}) AND $__timeFilter_ms(event_utc) AND ${rawMinutes}`)
    .replace('{{bucket}}', 'toStartOfMinute(row.4)')
    .replace('{{generation}}', 'toUInt64(0)');
}

export function states(filter: string): string {
  return `SELECT * FROM benchmark_minute_rollups WHERE ${filter}
 AND (region, cloud_provider, bucket, generation) IN (${committedMinutes})
 UNION ALL ${rawStates(filter)}`;
}

// Activity comes from provider summary rows, preserving observation times and
// subscription IDs. Boundary minutes use actual raw timestamps, not bucket edges.
export function activity(filter: string): string {
  return `SELECT region, cloud_provider, subscription_id, data_type, commitment, provider,
 first_seen, last_seen FROM benchmark_minute_rollups
 WHERE kind = 'provider' AND ${filter} AND (region, cloud_provider, bucket, generation) IN (${committedMinutes})
 UNION ALL
 SELECT region, cloud_provider, subscription_id, data_type, commitment, provider,
 min(event_utc) AS first_seen, max(event_utc) AS last_seen
 FROM benchmark_event_lags WHERE ${filter} AND $__timeFilter_ms(event_utc) AND ${rawMinutes}
 GROUP BY region, cloud_provider, subscription_id, data_type, commitment, provider`;
}
