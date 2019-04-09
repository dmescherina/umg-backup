SELECT
  b.stream_date,
  b.source_uri,
  b.testposition,
  b.httype,
  (LN(relposition) - xmean) / xstdev AS xstd,
  CASE ystdev
    WHEN 0 THEN 0
    ELSE (streams - ymean) / ystdev
  END AS ystd
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_HeadTail_Build2` b
INNER JOIN
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_stdev_estimates` ps
ON
  b.source_uri=ps.source_uri
  AND b.testposition =ps.testposition
  AND b.httype = ps.httype
  AND b.stream_date=ps.stream_date
INNER JOIN
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_HeadTail_mean_estimates` pm
ON
  b.source_uri=pm.source_uri
  AND b.testposition =pm.testposition
  AND b.httype = pm.httype
  AND b.stream_date=pm.stream_date