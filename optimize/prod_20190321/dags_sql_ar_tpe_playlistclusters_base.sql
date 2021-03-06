select source_uri,usercluster,streams,users,rank() over (partition by source_uri order by users desc) as crank
from (

select u.source_uri,cluster13 as usercluster,count(*) as streams,count(distinct s.user_id) as users 
from `umg-partner.spotify.streams` s
inner join `umg-comm-tech-dev.Optimize.ar_tpr_seed_uris` u 
on s.source_uri = u.new_source_uri 
left outer join `umg-data-science.spotify.clusters13` c
on c.user_id = s.user_id
where _PARTITIONTIME  between timestamp("{{ macros.ds_add( ds, -8) }}") and timestamp("{{ macros.ds_add(ds,-1) }}")
group by u.source_uri,cluster13) c