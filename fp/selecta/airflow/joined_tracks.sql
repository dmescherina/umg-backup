SELECT
  isrc,
  a.monthly_users AS monthly_users_apple,
  a.monthly_streams AS monthly_streams_apple,
  a.lean_back_perc AS lean_back_perc_apple,
  s.monthly_users AS monthly_users_spotify,
  s.monthly_streams AS monthly_streams_spotify,
  s.lean_back_perc AS lean_back_perc_spotify
FROM
  `umg-comm-tech-dev.fixed_playlists_selecta.tracks_apple` a
  JOIN `umg-comm-tech-dev.fixed_playlists_selecta.tracks` s
  using (isrc)