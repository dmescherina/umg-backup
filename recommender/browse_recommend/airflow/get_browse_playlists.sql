SELECT
  territory,
  category_id,
  Position,
  playlist_uri,
  playlist_name
FROM
  `umg-alpha.spotify.spotify_playlist_browse`
WHERE
  report_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND owner_id = "spotify"