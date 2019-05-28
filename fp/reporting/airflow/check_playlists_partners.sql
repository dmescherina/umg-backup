SELECT
  l.*,
IF
  (l.upc IN (
    SELECT
      upc
    FROM
      `umg-comm-tech-dev.fixed_playlists_data.streaming`
    WHERE
      source='spotify'
    GROUP BY
      upc),
    'Y',
    'N') AS in_spotify_streams,
IF
  (l.upc IN (
    SELECT
      upc
    FROM
      `umg-comm-tech-dev.fixed_playlists_data.streaming`
    WHERE
      source='deezer'
    GROUP BY
      upc),
    'Y',
    'N') AS in_deezer_streams,
IF
  (l.upc IN (
    SELECT
      upc
    FROM
      `umg-comm-tech-dev.fixed_playlists_data.streaming`
    WHERE
      source='apple_music'
    GROUP BY
      upc),
    'Y',
    'N') AS in_apple_music_streams,
IF
  (l.upc IN (
    SELECT
      upc
    FROM
      `umg-comm-tech-dev.fixed_playlists_data.consumption`
    GROUP BY
      upc),
    'Y',
    'N') AS in_financial_data,
IF
  (l.upc IN (
    SELECT
      upc
    FROM
      `umg-comm-tech-dev.fixed_playlists_data.streaming`
    WHERE
      source IN ('amazon_prime',
        'amazon_unlimited')
    GROUP BY
      upc),
    'Y',
    'N') AS in_amazon_data
FROM
  `umg-comm-tech-dev.fixed_playlists_data.playlists_list` l