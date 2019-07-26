SELECT
  v.user_id AS customer_id,
  v.stream_datetime AS stream_timestamp,
  v.source_uri,
  v.partner_track_id AS track_id,
  CONCAT(album_artist_name,':',track_name) as trackname,
  v.stream_source AS source,
  v.stream_duration AS length,
  v.stream_country_name AS country,
  cast(v._PARTITIONTIME as date) AS stream_date,
  RANK() OVER (PARTITION BY v.user_id, CONCAT(t.album_artist_name,':',t.track_name)
  ORDER BY
    v.stream_datetime) AS prank
FROM
  `umg-partner.spotify.streams` v
JOIN (
  SELECT
    DISTINCT user_id
  FROM
    `umg-partner.spotify.streams` s
  INNER JOIN
    `umg-comm-tech-dev.Optimize.ar_tpr_seed_uris` u
  ON
    REGEXP_EXTRACT(s.source_uri,r'playlist:(.*)') = REGEXP_EXTRACT(u.new_source_uri,r'playlist:(.*)')
  WHERE
    _PARTITIONTIME =  timestamp("{{ macros.ds_add(ds, -1) }}") ) s
ON
  s.user_id = v.user_id
LEFT JOIN
  `umg-edw.spotify.spotify_track_metadata` t
ON
v.partner_track_id = t.partner_track_id
WHERE
  v._PARTITIONTIME BETWEEN timestamp("{{ macros.ds_add( ds, -57) }}") and timestamp("{{ macros.ds_add(ds, -1) }}") 