SELECT
  s.stream_date,
  s.source_uri,
  AVG(testposition) changepoint,
  AVG(Error) error
FROM
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_find` s
WHERE
  erank = 1
GROUP BY
  s.stream_date,
  s.source_uri