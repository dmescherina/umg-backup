WITH
  no_opt AS(
  SELECT
    playlist_uri,
    owner_id,
    COUNT(requesttype) AS count_optimizations
  FROM
    `umg-comm-tech-dev.optimize_reporting.usage_owner`
  WHERE
    requesttype =0
  GROUP BY
    playlist_uri,
    owner_id),
  revenue AS (
  SELECT
    DISTINCT playlist_uri,
    owner_id,
    SUM(total_revenue) AS total_revenue,
    SUM(ad_funded_stream_revenue) AS ad_funded_stream_revenue,
    SUM(premium_stream_revenue) AS premium_stream_revenue
  FROM
    `umg-comm-tech-dev.optimize_reporting.optimize_playlist_stats` op
  JOIN (
    SELECT
      DISTINCT playlist_uri,
      owner_id
    FROM
      no_opt) opt
  ON
    REGEXP_EXTRACT(op.source_uri,r'playlist.(.*)') = REGEXP_EXTRACT(opt.playlist_uri,r'playlist.(.*)')
  GROUP BY
    playlist_uri,
    owner_id)
SELECT
  opt.playlist_uri,
  opt.owner_id,
  total_revenue,
  ad_funded_stream_revenue,
  premium_stream_revenue,
  count_optimizations
FROM
  revenue op
JOIN
  no_opt opt
ON
  REGEXP_EXTRACT(op.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(opt.playlist_uri,r'playlist.(.*)')
ORDER BY
  count_optimizations DESC
