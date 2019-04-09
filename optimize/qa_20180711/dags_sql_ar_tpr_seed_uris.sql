select playlist_uri as source_uri, concat("spotify:playlist:",d.playlist_id) as new_source_uri ,d.playlist_owner,max(playlist_name) as playlistname from
`umg-partner.spotify.playlist_history`  d
left outer join `umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup`   p
on d.playlist_owner = p.owner_id
left outer join `umg-comm-tech-dev.Optimize.ar_apollo_uri_lkup`  s
on d.playlist_uri = s.uri
where (d.playlist_owner like 'el_list%' 
or p.majortype = 'UMG' or s.uri is not null) and _PARTITIONTIME  between timestamp("{{ macros.ds_add( ds, -29) }}") and timestamp("{{ macros.ds_add(ds, -1) }}")
group by playlist_uri,d.playlist_owner, d.playlist_id