-- Leave one hour of safety before raw TTL can remove rows during a rebuild.
SELECT toString(d.bucket) AS bucket, d.revision AS revision, ifNull(c.generation, 0) AS generation
FROM (SELECT bucket, max(revision) AS revision FROM benchmark_dirty_minutes
 WHERE region = {region:String} AND cloud_provider = {cloud_provider:String}
 AND bucket < toStartOfMinute(now() - toIntervalMillisecond({settle_ms:UInt64})) + toIntervalMinute({include_open:UInt8})
 AND bucket >= now() - INTERVAL 5 HOUR
 GROUP BY bucket) d
LEFT JOIN (SELECT bucket, max(commits.generation) AS generation,
 argMax(commits.revision, commits.generation) AS revision
 FROM benchmark_rollup_commits AS commits
 WHERE region = {region:String} AND cloud_provider = {cloud_provider:String} GROUP BY bucket) c ON d.bucket = c.bucket
WHERE d.revision > ifNull(c.revision, 0) ORDER BY d.bucket LIMIT {limit:UInt64}
