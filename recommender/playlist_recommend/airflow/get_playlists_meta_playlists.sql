SELECT
  playlist_id,
  isrc,
  COUNT(DISTINCT pth.playlist_date) AS days_on_playlist,
  MAX(playlist_owner) AS playlist_owner,
  MAX(playlist_name) AS playlist_name,
  MAX(playlist_description) AS playlist_description
FROM
  `umg-partner.spotify.playlist_track_history` pth
LEFT JOIN
  `umg-partner.spotify.playlist_history` ph
USING
  (playlist_id)
WHERE
  pth._PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
  AND pth._PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND ph._PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
  AND ph._PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND playlist_id IN (
  SELECT
    playlist_uri
  FROM
    `umg-comm-tech-dev.recommender_playlists.all_playlists`
  GROUP BY
    playlist_uri)
  AND playlist_name IS NOT NULL
GROUP BY
  playlist_id,
  isrc