SELECT
  DISTINCT t._PARTITIONTIME AS reportdate,
  CONCAT('spotify:user:',p.owner_id,':playlist:',t.playlist_id) AS source_uri,
  t.track_position AS position,
  t.isrc,
  t.track_uri,
  CONCAT(track_artist,':',track_title) AS track,
  track_title AS justtrack,
  0 AS streams,
  0 AS skips,
  0 AS length,
  0 AS disc,
  0 AS col,
  0 AS ret,
  0 AS sup,
  '' AS httype,
  CASE
    WHEN t.track_position < changepoint THEN (halpha+hbeta*LN(t.track_position))
    ELSE (talpha+tbeta*LN(t.track_position))
  END AS baseline,
  0 AS skipsperc,
  0 AS skipsperf,
  0 AS lengthperf,
  0 AS discperf,
  0 AS colperf,
  0 AS retperf,
  0 AS supperf,
  0 AS skipsb4,
  0 AS skipsb4perf,
  0 AS skipsperfbench,
  0 AS colperfbench,
  '' AS usercluster,
  '' AS clustername,
  d.playlist_name,
  '' AS releaseyear,
  '' AS releasetype,
  '' AS PMainType,
  '' AS PMinorType,
  '' AS PSubType,
  d.playlist_owner AS owner,
  track_artist AS artist,
  '' AS DiscoveryType,
  '' AS PerfCode,
  '' AS PerfType,
  0 AS score,
  0 AS playlists,
  1 AS hottrack,
  0 AS hottrackscore
FROM
  `umg-partner.spotify.playlist_track_history` t
LEFT OUTER JOIN
  `umg-comm-tech-dev.Optimize.ar_tpr_final_data_store` w
ON
  REGEXP_EXTRACT(w.source_uri,r'playlist:(.*)') = t.playlist_id
  AND w.isrc = t.isrc
  AND CONCAT(track_artist,':',track_title) = w.track
JOIN
  `umg-partner.spotify.playlist_history` d
ON
  REGEXP_EXTRACT(t.playlist_uri,r'playlist:(.*)') = d.playlist_id
JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
ON
  d.playlist_owner = p.owner_id
JOIN
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Results2` s
ON
  t.playlist_id = REGEXP_EXTRACT(s.source_uri,r'playlist:(.*)')
  AND CAST(t._PARTITIONTIME AS date) =s.stream_date
WHERE
  w.track IS NULL
  AND t._PARTITIONTIME = "{{ macros.ds_add( ds, -1) }}"
  AND d._PARTITIONTIME = "{{ macros.ds_add( ds, -1) }}"