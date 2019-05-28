select source_uri,usercluster,streams,users,rank() over (partition by source_uri order by users desc) as crank
from (

select u.source_uri,cluster13 as usercluster,count(*) as streams,count(distinct s.user_id) as users 
from `umg-partner.spotify.streams` s
inner join `umg-comm-tech-dev.Optimize.ar_tpr_seed_uris` u 
on REGEXP_EXTRACT(s.source_uri,r'playlist:(.*)') = REGEXP_EXTRACT(u.new_source_uri,r'playlist:(.*)') 
left outer join `umg-alpha.spotify.swift_clusters13` c
on c.user_id = s.user_id
where _PARTITIONTIME  between timestamp("{{ macros.ds_add( ds, -8) }}") and timestamp("{{ macros.ds_add(ds,-1) }}")
group by u.source_uri,cluster13) c