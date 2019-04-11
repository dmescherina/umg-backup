SELECT
  b.stream_date,
  b.source_uri,
  b.testposition,
  MAX(relposition) as totaltracks,
  SUM(ABS(streams-(alpha+beta*LN(relposition)))) as Error,
  RANK() OVER (PARTITION BY b.stream_date, b.source_uri ORDER BY SUM(ABS(streams-(alpha+beta*LN(relposition)))) ASC) as ERank
FROM
  `umg-comm-tech-dev.Optimize.AR_TPR_HeadTail_Build2`  b
JOIN
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Slope` ps
ON
  REGEXP_EXTRACT(b.source_uri,r'playlist:(.*)')=REGEXP_EXTRACT(ps.source_uri,r'playlist:(.*)')
  AND b.testposition =ps.testposition
  AND b.httype = ps.httype
  AND b.stream_date=ps.stream_date
GROUP BY
  b.stream_date,
  b.source_uri,
  b.testposition
HAVING
  b.testposition < MAX(relposition)-10