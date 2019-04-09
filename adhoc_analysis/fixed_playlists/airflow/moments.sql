SELECT
m.report_date,
m.isrc,
e.original_release_date,
e.genre_name,
e.parent_genre_name,
e.major_label,
territory,
streams_1_week,
streams_15_week,
score_1_day,
score_7_day,
score_28_day 
FROM
  `umg-data-science.moments.spotify` m
  LEFT JOIN `umg-data-science.epf.song_label_view` e
  USING (isrc)
WHERE
  _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND m.isrc IS NOT NULL
  AND m.score_1_day >0
  AND m.score_7_day >0