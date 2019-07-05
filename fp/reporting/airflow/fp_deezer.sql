SELECT
  upc,
  MAX(pl.release_date) AS release_date,
  MAX(pl.release_title) AS release_title,
  MAX(pl.playlist_owner) AS release_owner,
  isrc,
  country_code AS country,
  stream_date,
  deezer_source_of_stream as stream_source,
  COUNT(transaction_time) AS streams,
  "deezer" AS source
FROM
  `umg-comm-tech-dev.fixed_playlists_data.playlists_list` pl
LEFT JOIN
  `umg-alpha.deezer.deezer_transactions` s
USING
  (upc)
WHERE
  stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND stream_date < DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
GROUP BY
  upc,
  isrc,
  country,
  stream_date,
  stream_source,
  source