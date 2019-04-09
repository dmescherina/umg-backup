SELECT
  playlist_uri AS source_uri,
  CONCAT("spotify:playlist:",d.playlist_id) AS new_source_uri,
  d.playlist_owner,
  MAX(playlist_name) AS playlistname
FROM
  `umg-partner.spotify.playlist_history` d
LEFT OUTER JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup` p
ON
  d.playlist_owner = p.owner_id
LEFT OUTER JOIN
  `umg-comm-tech-dev.Optimize.ar_apollo_uri_lkup` s
ON
  d.playlist_uri = s.uri
WHERE
  (d.playlist_owner LIKE 'el_list%'
    OR p.majortype = 'UMG'
    OR s.uri IS NOT NULL)
  AND _PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add( ds, -29) }}")
  AND TIMESTAMP("{{ macros.ds_add(ds, -1) }}")
GROUP BY
  playlist_uri,
  d.playlist_owner,
  d.playlist_id