select distinct a.*,  b.playlist_uri

from `umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild1a` a 


join `umg-data-science.spotify.playlist_uris` b


on REGEXP_EXTRACT(a.source_uri,'playlist.(.*)') = b.playlist_id