SELECT
  playlist_uri,
  isrc,
  COUNT(DISTINCT playlist_date) AS days_on_playlist,
  IF(track_artist IS NULL, artist_display_name, track_artist) AS track_artist,
  IF(track_title IS NULL, name, track_title) AS track_title
FROM
  `umg-partner.spotify.playlist_track_history`
LEFT JOIN
  `umg-alpha.epf.song_match`
USING
  (isrc)
LEFT JOIN
  `umg-alpha.epf.song`
USING
  (song_id)
WHERE
  _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
  AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND playlist_uri IN (
  SELECT
    playlist_uri
  FROM
    `umg-comm-tech-dev.recommender_model.all_playlists`
  GROUP BY
    playlist_uri)
  AND track_artist IS NOT NULL
  AND track_title IS NOT NULL
GROUP BY
  playlist_uri,
  isrc,
  track_artist,
  track_title