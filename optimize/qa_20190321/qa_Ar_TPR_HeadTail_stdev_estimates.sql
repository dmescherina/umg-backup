SELECT
  b.stream_date,
  b.source_uri,
  b.testposition,
  b.httype,
  CASE SUM(POW((LN(relposition) - xmean),2))
    WHEN 0 THEN 1
    ELSE SQRT(SUM(POW((LN(relposition) - xmean),2)) / (COUNT(*) - 1))
  END AS xstdev,
  SQRT(SUM(POW((streams - ymean),2)) / (COUNT(*) - 1)) AS ystdev
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_HeadTail_Build2` b
INNER JOIN
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_mean_estimates` pm
ON
  b.source_uri=pm.source_uri
  AND b.testposition =pm.testposition
  AND b.httype = pm.httype
  AND b.stream_date=pm.stream_date

JOIN (
  SELECT
    stream_date,
    source_uri,
    testposition,
    httype,
    COUNT(*) tot
  FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_HeadTail_Build2`
  GROUP BY
    stream_date,
    source_uri,
    testposition,
    httype
  HAVING
    COUNT(*) > 1 ) t
ON
  b.source_uri=t.source_uri
  AND b.testposition =t.testposition
  AND b.httype = t.httype
  AND b.stream_date=t.stream_date
GROUP BY
  stream_date,
  b.source_uri,
  b.testposition,
  b.httype