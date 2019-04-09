SELECT
  b.*
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild3` b
LEFT OUTER JOIN (
  SELECT
    stream_date,
    source_uri,
    AVG(streams) AS avgstreams
  FROM
    `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild3`
  GROUP BY
    stream_date,
    source_uri) p3
ON
  b.stream_date = p3.stream_date
  AND b.source_uri = p3.source_uri
WHERE
  streams > 0.1*avgstreams