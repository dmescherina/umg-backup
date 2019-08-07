WITH
  streams AS (
  SELECT
    *
  FROM
    `umg-edw.apple_music.streams`
  WHERE
    (stream_datetime >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      AND stream_datetime < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
    AND ifNull(content_owner,
      '') != 'Because Music')),
  track AS (
  SELECT
    *
  FROM
    `umg-edw.apple_music.content_metadata`),
  pl AS (
  SELECT
    upc,
    MAX(release_date) AS release_date,
    MAX(release_title) AS release_title,
    MAX(playlist_owner) AS release_owner
  FROM
    `umg-comm-tech-dev.fixed_playlists_data.playlists_list`
  GROUP BY
    upc)
SELECT
  REGEXP_EXTRACT(t.vendor_id,r'00\d{12}') AS upc,
  pl.release_date AS release_date,
  pl.release_title AS release_title,
  pl.release_owner AS release_owner,
  t.isrc AS isrc,
  s.user_country_code AS country,
  s.stream_date,
  s.apple_stream_source AS stream_source,
  COUNT(s.stream_datetime) AS streams,
  "apple_music" AS source
FROM
  streams s
JOIN
  track t
ON
  (t.apple_track_id = s.apple_track_id)
JOIN
  pl
ON
  REGEXP_EXTRACT(t.vendor_id,r'00\d{12}')=pl.upc
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8