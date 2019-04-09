SELECT
  isrc,
  SUM(total_user_count) AS monthly_users,
  SUM(total_stream_count) AS monthly_streams,
  SUM(lean_back_stream_count)/SUM(total_stream_count)*100 AS lean_back_perc
FROM
  `umg-partner.spotify.daily_track_history`
WHERE
  stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 33 DAY)
GROUP BY
  isrc