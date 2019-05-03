WITH
  napster_playlists AS (
  SELECT
    *
  FROM
    `umg-comm-tech-dev.deezer.create_in_napster`)
SELECT
  NULL AS deezer_id,
  parent_brand,
  brand,
  name AS playlist_title,
  TIMESTAMP("1900-01-01") AS creation_date,
  stream_date,
  CAST(COUNT(isrc)*0.005748134 AS INT64) AS streams,
  'napster_estimation' AS source
FROM
  napster_playlists
LEFT JOIN
  `umg-partner.spotify.streams` t
ON
  napster_playlists.spotify_id = REGEXP_EXTRACT(t.source_uri,r'playlist.(.*)')
WHERE
  t._PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  8