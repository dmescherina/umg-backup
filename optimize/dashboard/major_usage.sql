SELECT
  a.*,
  c.owner_id,
  c.majortype
FROM
  `umg-comm-tech-dev.optimize_usage.usage` a
JOIN
  `umg-partner.spotify.playlist_history` b
ON
  REGEXP_EXTRACT(a.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(b.playlist_uri,r'playlist.(.*)')
JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` c
ON
  b.playlist_owner = c.owner_id
WHERE
  _PARTITIONTIME = TIMESTAMP("{{ macros.ds_add( ds, -2) }}")