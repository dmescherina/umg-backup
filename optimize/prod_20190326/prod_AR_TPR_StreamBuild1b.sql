SELECT
  DISTINCT a.*,
  b.playlist_uri
FROM
  `umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild1a` a
JOIN
  `umg-alpha.spotify.playlist_uris` b
ON
  REGEXP_EXTRACT(a.source_uri,'playlist.(.*)') = b.playlist_id