SELECT
  playlist_uri AS source_uri,
  MAX(playlist_name) AS playlistname,
  FORMAT_TIMESTAMP("%E4Y%m%d", s._PARTITIONTIME) AS date,
  CAST(s._PARTITIONTIME AS timestamp) AS datetime,
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
JOIN
  `umg-partner.spotify.playlist_history` ph
ON
  REGEXP_EXTRACT(s.source_uri,'playlist.(.*)') = ph.playlist_id
JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
ON
  ph.playlist_owner = p.owner_id
WHERE
  REGEXP_EXTRACT(ph.playlist_uri,'playlist.(.*)') NOT IN (
  SELECT
    REGEXP_EXTRACT(playlist_uri,'playlist.(.*)')
  FROM
    `umg-comm-tech-dev.optimize_reporting.optimized_uris`)
  AND ph._PARTITIONTIME = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
  AND s._PARTITIONTIME = TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
  AND (p.majortype = 'UMG')
GROUP BY
  source_uri,
  date,
  datetime
