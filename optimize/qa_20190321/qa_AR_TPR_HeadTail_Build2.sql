SELECT
  b1.stream_date,
  b1.source_uri,
  b1.position as testposition,
  b2.position as relposition,
  b2.streams,
  CASE
    WHEN b1.position < b2.position THEN 'tail'
    ELSE 'head'
  END HTType
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild3b`  b1
LEFT JOIN
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild3b` b2
ON
  b1.source_uri = b2.source_uri
  and b1.stream_date = b2.stream_date
  where b1.position > 8 and b1.streams > 5 and b2.streams > 5