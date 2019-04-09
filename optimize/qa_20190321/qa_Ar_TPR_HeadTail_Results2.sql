SELECT
  b.stream_date,
  b.source_uri,
  changepoint,
  error,
  MAX(CASE
      WHEN httype = 'head' THEN alpha END)as  halpha, MAX(CASE
      WHEN httype = 'head' THEN beta END) as hbeta,
  MAX(CASE
      WHEN httype = 'tail' THEN alpha END) as talpha, MAX(CASE
      WHEN httype = 'tail' THEN beta END) as tbeta
FROM
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_Results1`  b
JOIN
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_Slope`  ps
ON
  b.source_uri=ps.source_uri
  AND b.changepoint =ps.testposition
  AND b.stream_date=ps.stream_date
GROUP BY
  b.stream_date,
  b.source_uri,
  changepoint,
  error