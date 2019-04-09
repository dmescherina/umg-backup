SELECT
customer_id,
  CONCAT(t.album_artist_name,':',t.track_name) as track,
  MIN(stream_timestamp) as start_date,
  MIN(CASE
      WHEN source = 'collection' THEN stream_timestamp END) as coldate,
  COUNT(*) as plays
FROM
  `umg-comm-tech-dev.Optimize.AR_TPR_StreamExport` b
LEFT JOIN
  `umg-partner.spotify.spotify_track_metadata` t
ON
  b.track_id = t.partner_track_id

GROUP BY
  customer_id,
  CONCAT(t.album_artist_name,':',t.track_name);