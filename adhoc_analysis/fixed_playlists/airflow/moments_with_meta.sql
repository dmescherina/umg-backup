SELECT
m.isrc,
MAX(d.artist_name) as artist_name,
MAX(d.track_name) as track_name,
m.original_release_date,
m.genre_name,
m.parent_genre_name,
m.major_label,
MAX(d.label) as spotify_label,
m.territory,
m.streams_1_week,
m.streams_15_week,
m.score_1_day,
m.score_7_day,
m.score_28_day 
FROM
  `umg-comm-tech-dev.fixed_playlists.moments` m
LEFT JOIN `umg-partner.spotify.spotify_track_metadata` d
  ON m.isrc=d.track_isrc
GROUP BY
1,4,5,6,7,9,10,11,12,13,14