DELETE FROM benchmark_minute_rollups
WHERE region = {region:String} AND cloud_provider = {cloud_provider:String}
 AND generation < {cutoff:UInt64}
 AND (bucket, generation) NOT IN (SELECT bucket, max(generation) FROM benchmark_rollup_commits
  WHERE region = {region:String} AND cloud_provider = {cloud_provider:String} GROUP BY bucket)
 AND bucket NOT IN (SELECT bucket FROM benchmark_rollup_commits
  WHERE region = {region:String} AND cloud_provider = {cloud_provider:String}
  GROUP BY bucket HAVING max(generation) >= {cutoff:UInt64})
