SELECT
  upc,
  MAX(pl.release_date) AS release_date,
  MAX(pl.release_title) AS release_title,
  MAX(pl.playlist_owner) AS playlist_owner,
  CASE
    WHEN sub_account_name ='7 Digital' THEN '7 Digital'
    WHEN sub_account_name = '7digital Limited' THEN '7 Digital'
    WHEN sub_account_name = 'Apple' THEN 'Apple'
    WHEN sub_account_name = 'iTunes' THEN 'iTunes'
    WHEN sub_account_name = 'Amazon' THEN 'Amazon'
    WHEN sub_account_name = 'Deezer' THEN 'Deezer'
    WHEN sub_account_name = 'Spotify' THEN 'Spotify'
    WHEN sub_account_name = 'YouTube' THEN 'YouTube'
    WHEN sub_account_name = 'Google' THEN 'Google'
    WHEN sub_account_name = 'Napster by Rhapsody' THEN 'Napster'
    ELSE 'Other Partners'
  END AS sub_account_name,
  transaction_source,
  EXTRACT(year
  FROM
    transaction_date) AS year,
  EXTRACT(month
  FROM
    transaction_date) AS month,
  euro_amount,
  SUM(units) AS total_streams
FROM
  `umg-comm-tech-dev.fixed_playlists_data.playlists_list` pl
LEFT JOIN
  `umg-edw.consumption.combined_transactions` ct
USING
  (upc)
WHERE
  transaction_date > "2017-01-01"
GROUP BY
  1,
  5,
  6,
  7,
  8,
  9