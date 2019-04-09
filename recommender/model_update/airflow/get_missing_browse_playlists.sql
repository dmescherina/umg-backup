SELECT
  playlist_uri
FROM
  `umg-tools.metadata.spotify_playlist_browse`
WHERE
  report_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND playlist_uri NOT IN (
  SELECT
    playlist_uri
  FROM
    `umg-comm-tech-dev.recommender_model.all_playlists`
  GROUP BY
    playlist_uri)