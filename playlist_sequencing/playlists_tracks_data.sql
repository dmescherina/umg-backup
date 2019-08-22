SELECT
  c.playlist_id,
  playlist_name,
  playlist_owner,
  nartists,
  nalbums,
  maxpos,
  ph.playlist_date,
  pt.track_position,
  isrc,
  pt.track_artist,
  pt.track_title,
  pt.track_add_timestamp,
  pt.track_add_by 
FROM
  `umg-comm-tech-dev.playlist_sequencing.playlists_create_spotify` c
LEFT JOIN
  `umg-partner.spotify.playlist_history` ph
USING
  (playlist_id)
  LEFT JOIN 
  `umg-partner.spotify.playlist_track_history` pt
  ON ph.playlist_id =pt.playlist_id 
  AND ph.playlist_date = pt.playlist_date
  WHERE ph.playlist_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY)
  AND ph.playlist_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)