SELECT
  parent_brand,
  brand,
  name,
  playlist_title,
  cp.deezer_id,
  dp.playlist_id,
  dp.creation_date
FROM
  `umg-comm-tech-dev.deezer.create_export` cp
JOIN
  `umg-alpha.deezer.deezer_playlists` dp
ON
  dp.playlist_id = cp.deezer_id
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7