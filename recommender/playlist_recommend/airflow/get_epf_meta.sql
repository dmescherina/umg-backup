SELECT
  isrc,
  MIN(track_artist) AS track_artist,
  MIN(track_title) AS track_title,
  label_studio,
  major_label,
  genre_name,
  parent_genre_name,
  original_release_date
FROM
  `umg-comm-tech-dev.recommender.model_data_with_meta`
LEFT JOIN
  `umg-alpha.epf.song_label_view`
USING
  (isrc)
GROUP BY
  isrc,
  label_studio,
  major_label,
  genre_name,
  parent_genre_name,
  original_release_date