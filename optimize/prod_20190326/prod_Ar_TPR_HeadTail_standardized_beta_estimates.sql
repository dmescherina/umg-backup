SELECT
  b.stream_date,
  b.source_uri,
  b.testposition,
  b.httype,
  CASE
    WHEN SUM(xstd * xstd) = 0 THEN 0
    ELSE SUM(xstd * ystd) / (COUNT(*) - 1)
  END AS betastd
FROM
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_standardized_data` b
GROUP BY
  b.stream_date,
  b.source_uri,
  b.testposition,
  b.httype;