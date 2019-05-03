SELECT
  d.playlist_uri,
  su.new_source_uri,
  d.playlist_owner,
  MAX(d.playlist_name) AS playlistname
FROM
  `umg-partner.spotify.playlist_history` d
JOIN
  `umg-comm-tech-dev.Optimize.ar_tpr_seed_uris` su
ON
  REGEXP_EXTRACT(d.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(su.source_uri,r'playlist.(.*)')
INNER JOIN
  `umg-comm-tech-dev.optimize_usage.usage` u
ON
  REGEXP_EXTRACT(u.playlist_uri,r'playlist.(.*)') = REGEXP_EXTRACT(d.playlist_uri,r'playlist.(.*)')
JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
ON
  d.playlist_owner = p.owner_id
WHERE
  (p.majortype = 'UMG')
  AND _PARTITIONTIME between TIMESTAMP("{{ macros.ds_add( ds, -35) }}") and TIMESTAMP("{{ macros.ds_add( ds, -1) }}")
  AND u.requesttype =0
GROUP BY
  playlist_uri,
  d.playlist_owner,
  su.new_source_uri
