SELECT
  upc,
  MAX(pl.release_date) AS release_date,
  MAX(pl.release_title) AS release_title,
  MAX(pl.playlist_owner) AS release_owner,
  isrc,
  user_country_code AS country,
  stream_date,
  stream_source,
  COUNT(stream_datetime) AS streams,
  "spotify" AS source
FROM
  `umg-comm-tech-dev.fixed_playlists_data.playlists_list` pl
LEFT JOIN
  `umg-edw.spotify.streams` s
USING
  (upc)
WHERE
  s.stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND s.stream_date < DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
GROUP BY
  upc,
  isrc,
  country,
  stream_date,
  stream_source,
  source