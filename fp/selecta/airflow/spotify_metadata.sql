SELECT
  track_isrc AS isrc,
  t.spotify_track_id,
  t.first_artist_spotify_id AS spotify_artist_id,
  t.duration_ms AS duration_spotify,
  t.popularity AS song_popularity_spotify,
  sa.popularity AS artist_popularity_spotify,
  followers AS artist_followers_spotify,
  genres as artist_genres_spotify,
  stm.artist_name AS artist_spotify,
  stm.track_name AS title_spotify,
  label AS label_spotify
FROM
  `umg-partner.spotify.spotify_track_metadata` stm
LEFT JOIN
  `umg-alpha.spotify_metadata.tracks` t
  ON stm.track_isrc = t.isrc
LEFT JOIN
  `umg-alpha.spotify_metadata.spotify_artists` sa
ON
  t.first_artist_spotify_id = sa.spotify_id
WHERE
  track_isrc IN (
  SELECT
    isrc
  FROM
    `umg-comm-tech-dev.fixed_playlists_selecta.tracks_joined`)