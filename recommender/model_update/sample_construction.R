library(dplyr)
library(bigrquery)

filt_followers<- 50
filt_weeklystreams<- 3500
filt_maxweeklystreams<-7*250000
rundate<-'23apr18'


#filt_followers<- 50
#filt_weeklystreams<- 3500
#filt_maxweeklystreams<-Inf
#rundate<-'16apr18'

#filt_followers<-20
#filt_weeklystreams<-700
#rundate<-'10apr18_deep'

#filt_followers<-100
#filt_weeklystreams<-7000
#rundate<-'23mar18'

if(FALSE){
  #read in popularity data
setwd('data')
files<-list.files(pattern='\\.data$')
setwd('..')
popdata<-NULL
for(i in 1:length(files)){
  print(paste0(i,' : ',files[i]))
  tmp<-read.table(paste0('data/',files[i]),sep='\t',quote='',comment.char='',fill=T,stringsAsFactors = F)
  popdata<-rbind(popdata,tmp)  
}
length(unique(popdata$V2))
colnames(popdata)<-c("sample_id","isrc","sample_market","track_uri","popularity")

#track popularity estimate from 20180317 model is :  
#intercept 1.4436 pop 0.1193 pop2 0.7649  (sinh) pop2=arcsinh (pop)
popdata$popularity[is.na(popdata$popularity)]<- -1
popdata$estimated_global_streams<-as.integer(sinh(1.4436+(popdata$popularity*0.1193)+(asinh(popdata$popularity)*0.7649)))
popdata$estimated_global_streams[popdata$popularity==-1]<-0
fx<-gzfile('data/track_popularity.csv.gz',open='w')
write.csv(popdata,file=fx,row.names = F)
close(fx)

fx<-gzfile('data/track_popularity.txt.gz',open='w')
write.table(popdata,file=fx,row.names = F,quote=F,col.names=T,sep="\t")
close(fx)

fx<-gzfile('data/isrcs.csv.gz',open='w')
isrcs<-unique(popdata$isrc)
write.csv(isrcs,file=fx,row.names = F)
close(fx)
rm(tmp,fx,i)
} else {
  
  popdata<-read.table(file=gzfile('data/track_popularity.txt.gz'),stringsAsFactors=F,fill=T,comment.char='',quote='',header=T,sep="\t")
  
}
pl_univ_sql<-paste0('with 
tracks as (select playlist_uri,artist_uri,album_uri,track_position
from `umg-partner.spotify.playlist_track_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01")),
playlists as (
select playlist_uri from  `umg-partner.spotify.playlist_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01") 
            and playlist_uri not like "spotify:user:spotify%" 
            and follower_count>=',filt_followers,' group by playlist_uri)
select playlist_uri,
count(distinct artist_uri) as nartists,
count(distinct album_uri) as nalbums,
max(track_position) as maxpos from tracks where playlist_uri in (select playlist_uri from playlists)
group by playlist_uri having nartists > 2 and nalbums > 2 and maxpos<=300')
                

isrc_univ_sql<-paste0('
select playlist_uri,isrc,count(distinct playlist_date) as days_on_playlist from 
`umg-partner.spotify.playlist_track_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01") and playlist_uri in
(select playlist_uri from  `umg-partner.spotify.playlist_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01") 
and playlist_uri not like "spotify:user:spotify%" 
and follower_count>=',filt_followers,') group by playlist_uri,isrc')
pl_catalog_sql<-'SELECT * FROM `umg-comm-tech-dev.playlist_pitching.accept_catalogue`'
pl_accept_sql<-'SELECT * FROM `umg-comm-tech-dev.playlist_pitching.active_playlists`'

pl_spotify<-query_exec('select * from `umg-data-science.playlist_pitching.target_playlists`',project='umg-data-science',use_legacy_sql = F,max_pages=Inf)
pl_univ<-query_exec(pl_univ_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)
isrc_univ<-query_exec(isrc_univ_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)

isrc_univ <-isrc_univ %>% filter(playlist_uri %in% pl_univ$playlist_uri)
pl_accept<-query_exec(pl_accept_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)
pl_catalog<-query_exec(pl_catalog_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)
isrc_epf_data<-query_exec('select * from `umg-data-science.playlist_pitching.playlisted_isrcs_2017_epf_data`',project = 'umg-data-science',use_legacy_sql = F, max_pages=Inf)
isrc_epf_data$original_release_date[is.na(isrc_epf_data$original_release_date)]<-as.Date('1900-01-01')
pl_catalog$count_frontline_tracks[is.na(pl_catalog$count_frontline_tracks)]<-0
pl_catalog$frac_catalogue[is.na(pl_catalog$frac_catalogue)]<-100

  list_cat<-pl_catalog$playlist_uri[pl_catalog$frac_catalogue>0.4]
targeted_spotify<-pl_accept$playlist_uri[pl_accept$playlist_uri %in%  list_cat]

sample_others<-pl_univ$playlist_uri[!(pl_univ$playlist_uri %in% pl_spotify$source_uri)]

sp_pl<-paste0("'",paste(targeted_spotify,collapse="','"),"'")


isrc_spot_sql<-paste0('select playlist_uri,isrc,count(distinct playlist_date) as days_on_playlist  
from `umg-partner.spotify.playlist_track_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01") and playlist_uri in
(',sp_pl,') 
 AND _PARTITIONTIME<timestamp("2018-01-01")  group by playlist_uri,isrc')
isrc_spot<-query_exec(isrc_spot_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)
isrc_spot<-isrc_spot %>% filter (playlist_uri %in% targeted_spotify)
isrc_univ<-isrc_univ %>% filter (playlist_uri %in% sample_others)

#now lets filter by owner
pl_owner_sql<-'select playlist_owner,count(distinct playlist_uri) as nplaylists,avg(follower_count) as mean_followers,max(follower_count) as max_followers from
`umg-partner.spotify.playlist_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01")
group by playlist_owner'
owner_univ<-query_exec(pl_owner_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)

owner_sample<-owner_univ %>% filter(mean_followers<10000,nplaylists<=30) %>% mutate(prefix=paste0('spotify:user:',playlist_owner,':')) 

pl_owner_list_sql<-'select playlist_uri,playlist_owner from
`umg-partner.spotify.playlist_history` 
where _PARTITIONTIME>=timestamp("2017-01-01") 
AND _PARTITIONTIME<timestamp("2018-01-01")
group by playlist_uri,playlist_owner'
owner_list<-query_exec(pl_owner_list_sql,project='umg-data-science',use_legacy_sql = F,max_pages=Inf)

owner_filt<-owner_list$playlist_uri[owner_list$playlist_owner %in% owner_sample$playlist_owner]
#save data for jaccard distance process - run on compute engine
#save(isrc_univ,isrc_spot,targeted_spotify,sample_others,file='jaccard.RData')

#post jaccard distance
#load('data/jaccard_dist.RData')
#hist(jaccard_dist$jaccard,breaks=100)
#summary(jaccard_dist)

#for this run no need to remove shadowing playlists as max jaccard < 85

selected_isrcs<-data.frame(isrc=unique(c(isrc_univ$isrc,isrc_spot$isrc)),stringsAsFactors = F)
selected_isrcs<-left_join(selected_isrcs,isrc_epf_data,by='isrc')
isrc_pop<-popdata %>% group_by(isrc) %>% summarise(globalstreams7day=sum(estimated_global_streams)) %>% ungroup()
selected_isrcs<-left_join(selected_isrcs,isrc_pop,by='isrc')
summary(selected_isrcs)
selected_isrcs$globalstreams7day[is.na(selected_isrcs$globalstreams7day)]<-0


#10apr18 run
#2003253 isrcs
#383148 isrcs > filt_weeklystreams global streams per week
#23mar18 run
#589483 isrcs
#175605 isrcs > filt_weeklystreams global streams per week

table(selected_isrcs$major_label)
table(selected_isrcs$major_label[selected_isrcs$globalstreams7day>filt_weeklystreams & selected_isrcs$globalstreams7day<filt_maxweeklystreams])

test_selection_isrcs<-
  selected_isrcs$isrc[selected_isrcs$globalstreams7day>filt_weeklystreams & selected_isrcs$globalstreams7day<filt_maxweeklystreams]

maxdays<-isrc_univ %>% group_by(playlist_uri) %>% 
  summarise(maxdays=max(days_on_playlist)) %>% ungroup %>% filter(maxdays>14)


test_spot<-isrc_spot %>% filter(isrc %in% test_selection_isrcs)

test_univ<-isrc_univ %>% filter(isrc %in% test_selection_isrcs)
test_univ<-test_univ %>% filter(playlist_uri %in% maxdays$playlist_uri)
test_univ<-test_univ %>% filter(playlist_uri %in% owner_filt)



#2281 spot and 152018 other playlists 23mar18
test_data<-rbind(test_spot,test_univ)

summary(test_data)
length(unique(test_data$playlist_uri)) #  121053 23apr18 121106 16apr18 130599 10apr18  18446 23mar18
length(unique(test_data$isrc)) #376620 23apr18 377053 16apr18 381683 10apr18 175605 23mar18

save(isrc_univ,isrc_spot,test_data,test_selection_isrcs,sample_others,targeted_spotify,selected_isrcs,popdata,isrc_epf_data,file=paste0('full_test',rundate,'_data.RData'))

save(selected_isrcs,test_data,file=paste0('test_data_',rundate,'.RData'))

fx<-gzfile(paste0('pp_playlist_isrc_test_data_',rundate,'.txt.gz'),open='w')
write.table(test_data,file=fx,row.names=F,sep="\t")
close(fx)

fx<-gzfile(paste0('pp_selected_isrcs_test_',rundate,'.txt.gz'),open='w')
write.table(selected_isrcs,file=fx,row.names=F,sep="\t")
close(fx)

fx<-gzfile(paste0('pp_selected_spotify_playlists_test_',rundate,'.txt.gz'),open='w')
write.table(targeted_spotify,file=fx,row.names=F,sep="\t")
close(fx)

fx<-gzfile(paste0('pp_selected_nonspotify_playlists_',rundate,'.txt.gz'),open='w')
write.table(sample_others,file=fx,row.names=F,sep="\t")
close(fx)






