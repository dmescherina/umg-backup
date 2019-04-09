select partner_track_id as track_id,stream_duration as length,rank() over (partition by partner_track_id order by streams desc) as Srank,streams
from (select partner_track_id,stream_duration,count(*) as streams 
from `umg-partner.spotify.streams`
where _PARTITIONTIME  = timestamp( "{{ macros.ds_add( ds, -1) }}")
group by partner_track_id,stream_duration) v;