WITH
  tracks AS (
  SELECT
    playlist_id AS playlist_uri,
    track_artist_id AS artist_uri,
    album_id AS album_uri,
    track_position
  FROM
    `umg-partner.apple_music.playlist_track_history`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
    AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))),
  playlists AS (
  SELECT
    playlist_id AS playlist_uri
  FROM
    `umg-partner.apple_music.playlist_history`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
    AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  GROUP BY
    playlist_uri)
SELECT
  playlist_uri,
  COUNT(DISTINCT artist_uri) AS nartists,
  COUNT(DISTINCT album_uri) AS nalbums,
  MAX(track_position) AS maxpos
FROM
  tracks
WHERE
  playlist_uri IN (
  SELECT
    playlist_uri
  FROM
    playlists)
GROUP BY
  playlist_uri
HAVING
  nartists > 3
  AND nalbums > 3
  AND maxpos<=500