SELECT
  cast(f.stream_date as date) as reportdate,
  f.source_uri,
  position,
  isrc ,
  track_uri,
  track,
  REGEXP_EXTRACT(track, r':(.*)') as justtrack,
  streams,
  skips,
  length,
  disc,
  col,
  ret,
  sup,
  httype,
  baseline,
  skipsperc,
  skipsperf,
  lengthperf,
  discperf,
  colperf,
  retperf,
  supperf,
  skipsb4,
  skipsb4perf,
  skipsperfbench,
  colperfbench,
  usercluster,
  CASE
    WHEN usercluster = 1 THEN 'Latin'
    WHEN usercluster = 2 THEN 'World'
    WHEN usercluster = 3 THEN 'Sunday Cafe'
    WHEN usercluster = 4 THEN 'Headliners'
    WHEN usercluster = 5 THEN 'Heavy Rock'
    WHEN usercluster = 6 THEN 'Pop'
    WHEN usercluster = 7 THEN 'Ibiza'
    WHEN usercluster = 8 THEN 'NOW'
    WHEN usercluster = 9 THEN 'Soccer Mom'
    WHEN usercluster = 10 THEN 'Alternative'
    WHEN usercluster = 11 THEN 'Urban'
    WHEN usercluster = 12 THEN 'Nashville'
    WHEN usercluster = 13 THEN 'Hip Hop' END as clustername,  pd.playlist_name as playlistname, CASE
    WHEN SUBSTR(isrc,6,2) > '20' THEN CONCAT('19',SUBSTR(isrc,6,2))
    ELSE CONCAT('20',SUBSTR(isrc,6,2))
  END as releaseyear,
  CASE
    WHEN avgrel IN (2018) THEN 'Frontline'
    WHEN avgrel IN (2017) THEN 'Carry Over'
    ELSE 'Catalogue' END as releasetype,
  majortype as PMainType,
  minortype as PMinorType,
  p.DESC as PSubType,
  pd.playlist_owner as owner,
  REGEXP_EXTRACT(track, r'(.*):') as artist,
  CASE
    WHEN disc < 0.3 THEN 'HITS'
    WHEN disc < 0.5 THEN 'Developing'
    ELSE 'Discovery' END as DiscoveryType,
  CONCAT(
    CASE
      WHEN skipsperfbench < 0 THEN 'L'
      ELSE 'H' END,
    CASE
      WHEN colperfbench < 0 THEN 'L'
      ELSE 'H' END,
    CASE
      WHEN skipsb4 < 0 THEN 'L'
      ELSE 'H' END) as PerfCode,
  CASE
    WHEN POW(POW(skipsperfbench,2)+POW(colperfbench,2),0.5)<0.04 THEN 'Average'
    WHEN skipsperfbench < 0
  AND colperfbench < 0
  AND skipsb4 < 0 THEN 'Passive Reaction'
    WHEN skipsperfbench > 0 AND colperfbench < 0 AND skipsb4 < 0 THEN 'Poor Performer'
    WHEN skipsperfbench < 0
  AND colperfbench > 0
  AND skipsb4 < 0 THEN 'Strong Performer'
    WHEN skipsperfbench > 0 AND colperfbench > 0 AND skipsb4 < 0 THEN 'Polarizing Reaction'
    WHEN skipsperfbench < 0
  AND colperfbench < 0
  AND skipsb4 > 0 THEN 'Passive Reaction'
    WHEN skipsperfbench > 0 AND colperfbench < 0 AND skipsb4 > 0 THEN 'Passive Reaction'
    WHEN skipsperfbench < 0
  AND colperfbench > 0
  AND skipsb4 > 0 THEN 'Strong Performer'
    WHEN skipsperfbench > 0 AND colperfbench > 0 AND skipsb4 > 0 THEN 'Polarizing Reaction' END as PerfType,  '' as score,  0 as playlists,  0 as hottrack,  0 as hottrackscore
    FROM `umg-comm-tech-dev.Optimize.ar_tpr_final_data_store` f
    LEFT OUTER JOIN (  SELECT cast(_PARTITIONTIME as date) AS pt,  playlist_uri,  MIN(CASE
        WHEN SUBSTR(isrc,6,2) > '20' THEN CONCAT('19',SUBSTR(isrc,6,2))
        ELSE CONCAT('20',SUBSTR(isrc,6,2)) END) AS minrel, AVG(CASE WHEN SUBSTR(isrc,6,2) > '20' THEN safe_cast(CONCAT('19',SUBSTR(isrc,6,2)) as int64)
        ELSE safe_CAST(CONCAT('20',SUBSTR(isrc,6,2)) AS int64) END) AS avgrel
  FROM
    `umg-partner.spotify.playlist_track_history`
  WHERE

  SUBSTR(isrc,6,2) > '0'
  GROUP BY
    pt,
    playlist_uri) d
ON
  f.source_uri = d.playlist_uri
  AND f.stream_date = d.pt
LEFT JOIN
  `umg-partner.spotify.playlist_history` pd
ON
  pd.playlist_uri = f.source_uri
  AND f.stream_date= cast(pd._PARTITIONTIME as date)
LEFT JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
ON
  pd.playlist_owner = p.owner_id
