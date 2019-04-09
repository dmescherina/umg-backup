WITH
  tracks AS (
  SELECT
    playlist_uri,
    artist_uri,
    album_uri,
    track_position
  FROM
    `umg-partner.spotify.playlist_track_history`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
    AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))),
  playlists AS (
  SELECT
    playlist_uri
  FROM
    `umg-partner.spotify.playlist_history`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 367 DAY))
    AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    AND follower_count>=10
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
  nartists > 2
  AND nalbums > 2
  AND maxpos<=500