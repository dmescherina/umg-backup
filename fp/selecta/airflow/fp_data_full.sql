SELECT
  *
FROM
  `umg-comm-tech-dev.fixed_playlists_selecta.data_iteration1`
LEFT JOIN
  `umg-comm-tech-dev.fixed_playlists_selecta.data_iteration2`
USING
  (isrc)
LEFT JOIN 
  `umg-comm-tech-dev.fixed_playlists_selecta.fp_isrc_count`
USING (isrc)
LEFT JOIN 
  `umg-comm-tech-dev.fixed_playlists_selecta.in_recommender`
USING (isrc)