SELECT
  f.stream_date,
  f.source_uri,
  position,
  ptrack_uri as track_uri,
  pisrc as isrc,
  title as track,
  streams,
  skips,
  length,
  disc/streams AS disc,
  col/nullif((streams-disc),0) as col ,
  ret/streams AS ret,
  sup/streams AS sup,
  CASE
    WHEN position < changepoint THEN 'head'
    ELSE 'tail'
  END AS httype,
  CASE
    WHEN position < changepoint THEN (halpha+hbeta*LN(position))
    ELSE (talpha+tbeta*LN(position))
  END AS baseline
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild3b` f
JOIN
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_Results2` s
ON
  f.stream_date=s.stream_date
  AND f.source_uri = s.source_uri