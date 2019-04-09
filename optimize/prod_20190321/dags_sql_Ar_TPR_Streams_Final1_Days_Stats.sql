SELECT
  f.stream_date,
  f.source_uri,
  position,
  track_uri,
  isrc,
  track,
  streams,
  skips,
  length,
  disc,
  col,
  ret,
  sup,
  httype,
  baseline,
  skips/nullif(streams,0) AS skipsperc,
  (skips/nullif(streams,0))/nullif(avgskips-1,0) AS skipsperf,
  length/nullif((avglength -1),0) AS lengthperf,
  disc/nullif((avgdisc-1),0) AS discperf,
  col/nullif((avgcol-1),0) AS colperf,
  ret/nullif((avgret-1),0) AS retperf,
  sup/nullif((avgsup-1),0) AS supperf,
  streams/nullif((baseline-1),0) AS skipsb4,
  (streams/nullif((baseline),0))/nullif((avgskipsb4-1),0) AS skipsb4perf
FROM
  `umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days`  f
JOIN (
  SELECT
    source_uri,
    AVG(skips/nullif(streams,0)) AS avgskips,
    AVG(length) AS avglength,
    AVG(disc) AS avgdisc,
    AVG(col) AS avgcol,
    AVG(ret) AS avgret,
    AVG(sup) AS avgsup,
    AVG(streams/nullif(baseline,0)) AS avgskipsb4
  FROM
     `umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days`
  GROUP BY
    source_uri) a
ON
  f.source_uri = a.source_uri;