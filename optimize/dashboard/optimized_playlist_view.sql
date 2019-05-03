WITH
  umg_optimized_playlist AS (
  SELECT
    d.playlist_uri,
    su.new_source_uri,
    d.playlist_owner,
    MAX(d.playlist_name) AS playlistname
  FROM
    `umg-partner.spotify.playlist_history` d
  JOIN
    `umg-comm-tech-dev.Optimize.ar_tpr_seed_uris` su
  ON
    REGEXP_EXTRACT(d.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(su.source_uri,r'playlist.(.*)')
  INNER JOIN
    `umg-comm-tech-dev.optimize_usage.usage` u
  ON
    REGEXP_EXTRACT(u.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(d.playlist_uri,r'playlist.(.*)')
  JOIN
    `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
  ON
    d.playlist_owner = p.owner_id
  WHERE
    (p.majortype = 'UMG')
    AND _PARTITIONTIME = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
    AND u.requesttype =0
  GROUP BY
    playlist_uri,
    d.playlist_owner,
    su.new_source_uri )
SELECT
  op.playlist_uri AS source_uri,
  playlistname,
  FORMAT_TIMESTAMP("%E4Y%m%d", _PARTITIONTIME) AS date,
  CAST(_PARTITIONTIME AS timestamp) AS datetime,
  COUNT(user_id) AS total_streams,
  COUNT(CASE
      WHEN revenue_model = 'Premium' THEN 1 END) AS premium_stream_count,  COUNT(CASE
      WHEN revenue_model = 'Ad-Funded' THEN 1 END) AS ad_funded_stream_count,
  COUNT(CASE
      WHEN revenue_model = 'Premium' THEN 1 END) * 0.0041 AS premium_stream_revenue,  COUNT(CASE
      WHEN revenue_model = 'Ad-Funded' THEN 1 END) * 0.0011 AS ad_funded_stream_revenue,
  (COUNT(CASE
        WHEN revenue_model = 'Ad-Funded' THEN 1 END) * 0.0011) + (COUNT(CASE
        WHEN revenue_model = 'Premium' THEN 1 END) * 0.0041) AS total_revenue
FROM
  `umg-partner.spotify.streams` s
INNER JOIN
  umg_optimized_playlist op
ON
  REGEXP_EXTRACT(op.new_source_uri,r'playlist.(.*)') = REGEXP_EXTRACT(s.source_uri,r'playlist.(.*)')
WHERE
  _PARTITIONTIME = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
GROUP BY
  source_uri,
  playlistname,
  date,
  datetime
