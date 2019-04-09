SELECT
  playlist_uri,
  isrc,
  COUNT(DISTINCT playlist_date) AS days_on_playlist,
  artist_name as track_artist,
  track_name as track_title
FROM
  `umg-partner.spotify.playlist_track_history`
LEFT JOIN `umg-partner.spotify.spotify_track_metadata`
ON `umg-partner.spotify.playlist_track_history`.isrc = `umg-partner.spotify.spotify_track_metadata`.track_isrc
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
GROUP BY
  playlist_uri,
  isrc,
  track_artist,
  track_title