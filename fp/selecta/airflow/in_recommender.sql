SELECT
  isrc,
  IF(isrc IN (
    SELECT
      isrc
    FROM
      `umg-comm-tech-dev.recommender.track_metadata_augm`
    GROUP BY
      isrc),
    1,
    0) AS in_recommender
FROM
  `umg-comm-tech-dev.fixed_playlists_selecta.data_iteration1`
GROUP BY
  isrc