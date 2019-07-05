SELECT
  *
FROM
  `umg-comm-tech-dev.store_checker.applemusic`
  WHERE Date = DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
UNION ALL
SELECT
  *
FROM
  `umg-comm-tech-dev.store_checker.applemusicmobile`
  WHERE Date = DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)