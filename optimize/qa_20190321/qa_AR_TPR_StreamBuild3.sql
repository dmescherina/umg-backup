SELECT
  b.playlist_uri AS source_uri,
  SUM(b.streams) streams,
  SUM(b.skips) skips,
  AVG(b.length) length,
  SUM(disc) disc,
  SUM(col) col,
  SUM(returning) ret,
  SUM(superfans) sup,
  p.track_position AS position,
  MAX(p.track_uri) AS ptrack_uri,
  CONCAT(MAX(p.track_artist),':',MAX(p.track_title)) AS title,
  MAX(p.isrc) AS pisrc,
  b.stream_date
FROM
  `umg-comm-tech-dev.optimize_qa.AR_TPR_StreamBuild1b` b
LEFT JOIN
  `umg-partner.spotify.playlist_track_history` p
ON
  b.stream_date = CAST(p._PARTITIONTIME AS date)
  AND p.playlist_id = REGEXP_EXTRACT(b.source_uri,r'playlist:(.*)')
  AND b.isrc = p.isrc
LEFT JOIN (
  SELECT
    isrc,
    MAX(formatted_title) canopustrack
  FROM
    `umg-data-science.canopus.resource_artist`
  GROUP BY
    isrc) c
ON
  c.isrc = p.isrc
WHERE
  CASE
    WHEN b.isrc = p.isrc THEN 1
    WHEN (b.track = CONCAT(p.track_artist,':',p.track_title)) THEN 1
    WHEN b.track = CONCAT(p.track_artist,':',c.canopustrack) THEN 1
    WHEN b.uri = p.track_uri THEN 1
    ELSE 0
  END = 1
GROUP BY
  b.playlist_uri,
  p.track_position,
  b.stream_date