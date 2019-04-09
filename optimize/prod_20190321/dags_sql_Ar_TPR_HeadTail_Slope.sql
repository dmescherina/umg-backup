SELECT
  b.stream_date,
  b.source_uri,
  b.testposition,
  b.httype,
  ymean - xmean * betastd * ystdev / xstdev AS Alpha,
  betastd * ystdev / xstdev AS Beta
FROM
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_standardized_beta_estimates` b
INNER JOIN
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_stdev_estimates` ps
ON
  b.source_uri=ps.source_uri
  AND b.testposition =ps.testposition
  AND b.httype = ps.httype
  AND b.stream_date=ps.stream_date
INNER JOIN
  `umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_mean_estimates` pm
ON
  b.source_uri=pm.source_uri
  AND b.testposition =pm.testposition
  AND b.httype = pm.httype
  AND b.stream_date=pm.stream_date