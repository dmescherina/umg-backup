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
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Results1`  b
JOIN
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Slope`  ps
ON
  REGEXP_EXTRACT(b.source_uri,r'playlist:(.*)')=REGEXP_EXTRACT(ps.source_uri,r'playlist:(.*)')
  AND b.changepoint =ps.testposition
  AND b.stream_date=ps.stream_date
GROUP BY
  b.stream_date,
  b.source_uri,
  changepoint,
  error