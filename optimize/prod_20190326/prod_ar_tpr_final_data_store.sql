select * from `umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_stats2_Bench` 
where stream_date  =  "{{ macros.ds_add( ds, -1) }}"