SELECT DISTINCT

t._PARTITIONTIME as reportdate,
t.playlist_uri as source_uri  ,
t.track_position as position ,
t.isrc ,
t.track_uri ,
CONCAT(track_artist,':',track_title) as track ,
track_title as justtrack,
0 as streams,
0 as skips,
0 as length,
0 as disc,
0 as col,
0 as ret,
0 as sup,
'' as httype,
CASE WHEN t.track_position < changepoint THEN (halpha+hbeta*LN(t.track_position))
ELSE (talpha+tbeta*LN(t.track_position)) END as baseline,
0 as skipsperc,
0 as skipsperf,
0 as lengthperf,
0 as discperf,
0 as colperf,
0 as retperf,
0 as supperf,
0 as skipsb4,
0 as skipsb4perf,
0 as skipsperfbench,
0 as colperfbench,
'' as usercluster,
'' as clustername,
d.playlist_name,
'' as releaseyear,
'' as releasetype,
'' as PMainType,
'' as PMinorType,
'' as PSubType,
d.playlist_owner as owner ,
track_artist as artist,
'' as DiscoveryType,
'' as PerfCode,
'' as PerfType,
0 as score,
0 as playlists,
1 as hottrack,
0 as hottrackscore
FROM `umg-partner.spotify.playlist_track_history`  t
LEFT OUTER JOIN `umg-comm-tech-dev.Optimize.ar_tpr_final_data_store`  w
ON w.source_uri = t.playlist_uri AND CONCAT(track_artist,':',track_title) = w.track
JOIN `umg-partner.spotify.playlist_history`  d
ON t.playlist_uri = d.playlist_uri
JOIN `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup`  p
ON d.playlist_owner = p.owner_id
JOIN `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Results2`  s
ON t.playlist_uri = s.source_uri AND cast(t._PARTITIONTIME as date) =s.stream_date
WHERE w.track IS NULL AND t._PARTITIONTIME  = "{{ macros.ds_add( ds, -1) }}" AND d._PARTITIONTIME =  "{{ macros.ds_add( ds, -1) }}"
