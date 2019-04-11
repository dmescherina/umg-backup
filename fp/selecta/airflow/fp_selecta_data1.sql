SELECT
t.*,
MAX(am.artist_apple) AS artist_apple,
MAX(sm.artist_spotify) AS artist_spotify,
MAX(am.title_apple) AS title_apple,
MAX(sm.title_spotify) AS title_spotify,
MAX(am.label_apple) AS label_apple,
MAX(am.major_label) AS major_label,
MAX(sm.label_spotify) AS label_spotify,
MAX(am.release_date) AS release_date,
MAX(am.master_track_release_date) AS master_release_date,
MAX(am.genre_apple) AS genre_apple,
MAX(am.parent_genre_apple) AS parent_genre_apple,
MAX(genre_r2) AS genre_r2,
MAX(sm.artist_genres_spotify) AS artist_genres_spotify,
MAX(sm.artist_followers_spotify) AS artist_followers_spotify,
MAX(sm.artist_popularity_spotify) AS artist_popularity_spotify,
MAX(sm.song_popularity_spotify) AS song_popularity_spotify,
MAX(am.track_popularity_apple) AS song_popularity_apple,
MAX(am.duration_apple) AS duration_apple,
MAX(sm.duration_spotify) AS duration_spotify
FROM
  `umg-comm-tech-dev.fixed_playlists_selecta.tracks_joined` t
LEFT JOIN
  `umg-comm-tech-dev.fixed_playlists_selecta.spotify_metadata` sm
USING
  (isrc)
LEFT JOIN
  `umg-comm-tech-dev.fixed_playlists_selecta.apple_r2_metadata` am
USING
  (isrc)
  GROUP BY t.isrc,
  t.monthly_users_apple,
  t.monthly_streams_apple,
  t.lean_back_perc_apple,
  t.monthly_users_spotify,
  t.monthly_streams_spotify,
  t.lean_back_perc_spotify 