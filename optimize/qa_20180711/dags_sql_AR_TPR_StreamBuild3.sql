select
b.playlist_uri as source_uri,
sum(b.streams) streams,
sum(b.skips) skips,
avg(b.length) length,
sum(disc) disc,
sum(col) col,
sum(returning) ret,
sum(superfans) sup,
p.track_position as position,
max(p.track_uri) as ptrack_uri ,
concat(max(p.track_artist),':',max(p.track_title)) as title,
max(p.isrc) as pisrc,
b.stream_date
from `umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild1b` b
left join `umg-partner.spotify.playlist_track_history` p
on b.stream_date = cast(p._PARTITIONTIME as date)
and b.playlist_uri = p.playlist_uri
left join (select isrc,max(formatted_title) canopustrack from `umg-data-science.canopus.resource_artist` group by isrc) c
on c.isrc = p.isrc
WHERE
    CASE
      WHEN b.isrc = p.isrc THEN 1
      WHEN (b.track = CONCAT(p.track_artist,':',p.track_title)) THEN 1
      WHEN b.track = CONCAT(p.track_artist,':',c.canopustrack) THEN 1
      WHEN b.uri = p.track_uri THEN 1
      ELSE 0
    END = 1
group by 
    b.playlist_uri,
    p.track_position,
    b.stream_date