SELECT
  upc,
  MAX(pl.release_date),
  MAX(pl.release_title),
  MAX(pl.playlist_owner),
  sub_account_name,
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
  `umg-swift.consumption.combined_transactions` ct
USING
  (upc)
WHERE
  sub_account_name IN ('7 Digital',
    '7digital Limited',
    'Apple',
    'iTunes',
    'Amazon',
    'Deezer',
    'Spotify',
    'YouTube',
    'Google',
    'Napster by Rhapsody')
GROUP BY
  1,
  5,
  6,
  7,
  8,
  9