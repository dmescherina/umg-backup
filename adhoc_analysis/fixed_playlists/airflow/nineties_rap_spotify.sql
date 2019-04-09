SELECT
stream_date,
stream_source,
count(stream_datetime) as streams
FROM
  `umg-partner.spotify.streams`
WHERE
  _PARTITIONTIME >= "2019-02-22 00:00:00"
  AND _PARTITIONTIME <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
AND upc='00600406839107'
GROUP BY 
stream_date,
stream_source