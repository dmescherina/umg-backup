SELECT
stream_date,
deezer_source_of_stream,
count(stream_date) as streams
FROM
  `umg-alpha.deezer.deezer_transactions` 
WHERE
  stream_date >= "2019-03-11"
  AND stream_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND upc='00600406839114'
GROUP BY 
stream_date,
deezer_source_of_stream 