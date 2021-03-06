SELECT
  e.source_uri,
  COUNT(*) AS streams,
  SUM(CASE
      WHEN e.length < 0.95*l.length THEN 1
      ELSE 0 END) AS skips,
  CAST(AVG(e.length) AS FLOAT64)/AVG(L.LENGTH) AS length,
  SUM(CASE
      WHEN stream_timestamp = c.start_date THEN 1
      ELSE 0 END) AS disc,
  SUM(CASE
      WHEN stream_timestamp > c.coldate THEN 1
      ELSE 0 END) AS col,
  sum(case when DATE_DIFF(cast(stream_timestamp AS DATE),cast(re.startdate AS DATE),DAY) > 0 then 1 else 0 end) as returning,
  SUM(CASE
      WHEN re.dailystreams > 10 THEN 1
      ELSE 0 END) AS superfans,
  e.uri,
  e.track,
  e.isrc,
  r.title,
  e.stream_date
FROM (
  SELECT
    v.*,
    CONCAT(t.album_artist_name,':',t.track_name) track,
    t.partner_track_uri uri,
    t.track_isrc isrc
  FROM
    `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamExport`  v
  INNER JOIN
    `umg-comm-tech-dev.optimize_qa.ar_tpr_seed_uris`  d
  ON
    v.source_uri = d.new_source_uri
  JOIN
    `umg-partner.spotify.spotify_track_metadata`  t
  ON
     v.track_id = t.partner_track_id

  where v.stream_date = "{{ macros.ds_add( ds, -1) }}") e
JOIN
 `umg-comm-tech-dev.optimize_qa.AR_TPR_TrackSkipLkup`  l
ON
  l.track_id = e.track_id
JOIN
  `umg-comm-tech-dev.optimize_qa.AR_TPR_UserDiscCol`  c
ON
  e.customer_id = c.customer_id
  AND e.track = c.track
LEFT JOIN (
  SELECT
    isrc,
    MAX(formatted_title) title
  FROM
    `umg-data-science.canopus.resource_artist` 
  GROUP BY
    isrc) r
ON
  e.isrc = r.isrc
JOIN
  `umg-comm-tech-dev.optimize_qa.AR_TPR_UserReturning`  re
ON
  e.customer_id = re.customer_id
  AND e.source_uri = re.source_uri
WHERE
  srank = 1
GROUP BY
  e.source_uri,
  e.uri,
  e.track,
  e.isrc,
  r.title,
  e.stream_date
