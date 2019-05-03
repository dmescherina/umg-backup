SELECT
  'optimized' AS opt_flag,
  *
FROM
  `umg-comm-tech-dev.optimize_reporting.optimize_playlist_stats`
WHERE
  Datetime = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
UNION ALL
SELECT
  'non-optimized' AS opt_flag,
  *
FROM
  `umg-comm-tech-dev.optimize_reporting.non_optimize_playlist_stats`
WHERE
  Datetime = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
