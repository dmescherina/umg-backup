WITH
  deezer_playlists AS (
  SELECT
    *
  FROM
    `umg-comm-tech-dev.deezer.create_in_deezer`)
SELECT
  deezer_id,
  parent_brand,
  brand,
  playlist_title,
  creation_date,
  stream_date,
  COUNT(isrc) AS streams,
  'deezer' AS source
FROM
  deezer_playlists
LEFT JOIN
  `umg-alpha.deezer.deezer_transactions` t
ON
  deezer_playlists.deezer_id = t.playlist_id
WHERE
  t.stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  8