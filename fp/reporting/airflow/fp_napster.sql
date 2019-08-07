SELECT
  upc,
  MAX(pl.release_date) AS release_date,
  MAX(pl.release_title) AS release_title,
  MAX(pl.playlist_owner) AS release_owner,
  isrc,
  s.iso2countrycode AS country,
  umg_report_date AS stream_date,
  s.sourceofstream as stream_source,
  COUNT(transactiontime) AS streams,
  "napster" AS source
FROM
  `umg-comm-tech-dev.fixed_playlists_data.playlists_list` pl
LEFT JOIN
  `umg-alpha.rhapsody.streams_v1` s
USING
  (upc)
WHERE
  umg_report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND umg_report_date < DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
GROUP BY
  upc,
  isrc,
  country,
  stream_date,
  stream_source,
  source