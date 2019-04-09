SELECT
  CAST(f.stream_date AS date) AS reportdate,
  CONCAT('spotify:user:',pd.playlist_owner,':playlist:',pd.playlist_id) AS source_uri,
  position,
  isrc,
  track_uri,
  track,
  REGEXP_EXTRACT(track, r':(.*)') AS justtrack,
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
    WHEN usercluster = 13 THEN 'Hip Hop'
  END AS clustername,
  pd.playlist_name AS playlistname,
  CASE
    WHEN SUBSTR(isrc,6,2) > '20' THEN CONCAT('19',SUBSTR(isrc,6,2))
    ELSE CONCAT('20',SUBSTR(isrc,6,2))
  END AS releaseyear,
  CASE
    WHEN avgrel IN (2018) THEN 'Frontline'
    WHEN avgrel IN (2017) THEN 'Carry Over'
    ELSE 'Catalogue'
  END AS releasetype,
  majortype AS PMainType,
  minortype AS PMinorType,
  p.DESC AS PSubType,
  pd.playlist_owner AS owner,
  REGEXP_EXTRACT(track, r'(.*):') AS artist,
  CASE
    WHEN disc < 0.3 THEN 'HITS'
    WHEN disc < 0.5 THEN 'Developing'
    ELSE 'Discovery'
  END AS DiscoveryType,
  CONCAT(
    CASE
      WHEN skipsperfbench < 0 THEN 'L'
      ELSE 'H' END,
    CASE
      WHEN colperfbench < 0 THEN 'L'
      ELSE 'H' END,
    CASE
      WHEN skipsb4 < 0 THEN 'L'
      ELSE 'H' END) AS PerfCode,
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
    WHEN skipsperfbench > 0 AND colperfbench > 0 AND skipsb4 > 0 THEN 'Polarizing Reaction'
  END AS PerfType,
  '' AS score,
  0 AS playlists,
  0 AS hottrack,
  0 AS hottrackscore
FROM
  `umg-comm-tech-dev.optimize_qa.ar_tpr_final_data_store` f
LEFT OUTER JOIN (
  SELECT
    CAST(_PARTITIONTIME AS date) AS pt,
    playlist_uri,
    playlist_id,
    MIN(CASE
        WHEN SUBSTR(isrc,6,2) > '20' THEN CONCAT('19',SUBSTR(isrc,6,2))
        ELSE CONCAT('20',SUBSTR(isrc,6,2)) END) AS minrel,
    AVG(CASE
        WHEN SUBSTR(isrc,6,2) > '20' THEN safe_CAST(CONCAT('19',SUBSTR(isrc,6,2)) AS int64)
        ELSE safe_CAST(CONCAT('20',SUBSTR(isrc,6,2)) AS int64) END) AS avgrel
  FROM
    `umg-partner.spotify.playlist_track_history`
  WHERE
    SUBSTR(isrc,6,2) > '0'
  GROUP BY
    pt,
    playlist_id,
    playlist_uri) d
ON
  REGEXP_EXTRACT(f.source_uri,r'playlist:(.*)') = d.playlist_id
  AND f.stream_date = d.pt
LEFT JOIN
  `umg-partner.spotify.playlist_history` pd
ON
  pd.playlist_uri = d.playlist_uri
  AND f.stream_date= CAST(pd._PARTITIONTIME AS date)
LEFT JOIN
  `umg-comm-tech-dev.optimize_qa.ar_apollo_owner_lkup` p
ON
  pd.playlist_owner = p.owner_id