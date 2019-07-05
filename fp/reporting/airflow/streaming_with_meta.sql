SELECT
  s.upc,
  s.release_date,
  s.release_title,
  s.release_owner,
  s.isrc,
  s.country,
  s.stream_date,
  s.stream_source,
  s.streams,
  s.source,
  MAX(m.artist_name) AS track_artist,
  MAX(track_name) AS track_title
FROM
  `umg-comm-tech-dev.fixed_playlists_data.streaming` s
LEFT JOIN
  `umg-partner.spotify.spotify_track_metadata` m
ON
  s.isrc = m.track_isrc
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10