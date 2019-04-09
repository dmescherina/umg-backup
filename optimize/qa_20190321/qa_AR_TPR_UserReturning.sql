SELECT
  customer_id,
  source_uri,
  MIN(stream_timestamp) as startdate,
  CAST(COUNT(*) AS FLOAT64)/COUNT(DISTINCT stream_date) as dailystreams
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamExport` b
GROUP BY
  customer_id,
  source_uri;