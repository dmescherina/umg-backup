select b.source_uri,b.usercluster,streams,users, crank,users/tusers userperc,globalperc,
rank() over (partition by b.source_uri order by (users/tusers)/globalperc desc) as userindex
,rank() over (partition by b.source_uri order by streams/users desc) as ausindex from `umg-comm-tech-dev.optimize_qa.ar_tpe_playlistclusters_base` b
join (select source_uri,sum(users) as tusers from `umg-comm-tech-dev.optimize_qa.ar_tpe_playlistclusters_base` group by source_uri) t
on b.source_uri = t.source_uri
join
(select usercluster,sum(users)/gtusers globalperc
from `umg-comm-tech-dev.optimize_qa.ar_tpe_playlistclusters_base` b
,(select sum(users) gtusers from `umg-comm-tech-dev.optimize_qa.ar_tpe_playlistclusters_base`) t2
 group by usercluster,gtusers) g
on g.usercluster = b.usercluster
where users/tusers > 0.05 and b.usercluster is not null;
