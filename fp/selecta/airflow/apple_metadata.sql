SELECT
  isrc,
  artist_name as artist_apple,
  title as title_apple,
  label_studio AS label_apple,
  major_label,
  slw.original_release_date AS release_date,
  earliest_master_track_release_date AS master_track_release_date,
  slw.genre_name AS genre_apple,
  slw.parent_genre_name AS parent_genre_apple,
  p.genre_name AS genre_r2,
  dcm.duration as duration_apple,
  song_rank as track_popularity_apple
FROM
  `umg-alpha.epf.song_label_view` slw
LEFT JOIN
  `umg-marketing.metadata.product` p
USING
  (isrc)
LEFT JOIN
  `umg-alpha.epf.song_match`
USING
  (isrc)
LEFT JOIN
  `umg-alpha.epf.song` s
USING
  (song_id)
LEFT JOIN
  `umg-alpha.epf.song_popularity_per_genre` sp
USING
  (song_id)
LEFT JOIN
`umg-partner.apple_music.daily_content_metadata` dcm
USING (isrc)
WHERE
  slw.isrc IN (
  SELECT
    isrc
  FROM
    `umg-comm-tech-dev.fixed_playlists_selecta.tracks_joined`)