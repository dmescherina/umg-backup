SELECT
  b.*
FROM
  `umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild3` b
LEFT OUTER JOIN (
  SELECT
    stream_date,
    source_uri,
    AVG(streams) AS avgstreams
  FROM
    `umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild3`
  GROUP BY
    stream_date,
    source_uri) p3
ON
  b.stream_date = p3.stream_date
  AND REGEXP_EXTRACT(b.source_uri,r'playlist:(.*)') = REGEXP_EXTRACT(p3.source_uri,r'playlist:(.*)')
WHERE
  streams > 0.1*avgstreams