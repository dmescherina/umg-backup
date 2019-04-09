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
  skips/nullif(streams,0) as skipsperc,
  (skips/nullif(streams,0))-avgskips as skipsperf,
  length-avglength as lengthperf,
  disc-avgdisc as discperf,
  col-avgcol as colperf,
  ret-avgret as retperf,
  sup-avgsup as supperf,
  streams/nullif(baseline,0)-1 as skipsb4,
  (streams/nullif(baseline,0))-avgskipsb4 as skipsb4perf
FROM
   `umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_Stats`  f
JOIN (
  SELECT
    source_uri,
    AVG(skips/nullif(streams,0)) avgskips,
    AVG(length) avglength,
    AVG(disc) avgdisc,
    AVG(col) avgcol,
    AVG(ret) avgret,
    AVG(sup) avgsup,
    AVG(streams/nullif(baseline,0)) avgskipsb4
  FROM
     `umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_Stats` 
  GROUP BY
    source_uri) a
ON
  f.source_uri = a.source_uri;