SELECT
  isrc,
  COUNT(DISTINCT CONCAT(isrc,':',upc)) AS album_count_spotify,
  COUNT(DISTINCT CONCAT(isrc,':',source_uri)) AS playlist_count_spotify,
  CASE
    WHEN monthly_streams_spotify >= PERCENTILE_CONT(monthly_streams_spotify,  0.05) OVER() THEN 'top5'
    WHEN monthly_streams_spotify < PERCENTILE_CONT(monthly_streams_spotify,
    0.05) OVER()
  AND monthly_streams_spotify >= PERCENTILE_CONT(monthly_streams_spotify,
    0.1) OVER() THEN 'top10'
    WHEN monthly_streams_spotify < PERCENTILE_CONT(monthly_streams_spotify,  0.1) OVER() AND monthly_streams_spotify >= PERCENTILE_CONT(monthly_streams_spotify,  0.25) OVER() THEN 'head'
    WHEN monthly_streams_spotify < PERCENTILE_CONT(monthly_streams_spotify,
    0.25) OVER()
  AND monthly_streams_spotify >= PERCENTILE_CONT(monthly_streams_spotify,
    0.75) OVER() THEN 'body'
    ELSE 'tail'
  END AS percentile_spotify
FROM
  `umg-comm-tech-dev.fixed_playlists_selecta.data_iteration1` d
LEFT JOIN
  `umg-partner.spotify.streams` s
USING
  (isrc)
WHERE
  s._PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY))
  AND s._PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
GROUP BY
  isrc,
  monthly_streams_spotify,
  monthly_streams_apple