SELECT
  isrc,
  COUNT(DISTINCT CONCAT(isrc,':',upc)) AS fixed_playlist_count_spotify
FROM
  `umg-partner.spotify.streams`
WHERE
  _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY))
  AND _PARTITIONTIME < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND isrc IN (
  SELECT
    isrc
  FROM
    `umg-comm-tech-dev.fixed_playlists_selecta.data_iteration1`
  GROUP BY
    isrc)
  AND upc IN (
  SELECT
    upc
  FROM
    `umg-comm-tech-dev.fixed_playlists_data.playlists_list`
  GROUP BY
    upc)
GROUP BY
  isrc