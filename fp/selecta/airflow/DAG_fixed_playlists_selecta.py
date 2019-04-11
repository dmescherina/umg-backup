import airflow
import os
import pytz
from airflow.models import BaseOperator
from datetime import date, timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

start_date = datetime(2019, 4, 1 , 0, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)

}


dag = DAG('Selecta',
  description = 'Data gathering and statistics creation for Selecta',
  schedule_interval='0 7 * * 1',
  max_active_runs=1,
  default_args=default_args)


#Gets the list of Spotify ISRCs, users and listens for the last month

tracks_spotify = BigQueryOperator(
  task_id='tracks_spotify',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/tarcks_spotify.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.tracks',
  dag = dag
    )


#Gets the list of Apple ISRCs, users and listens for the last month

tracks_apple = BigQueryOperator(
  task_id='tracks_apple',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/tarcks_apple.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.tracks_apple',
  dag = dag
    )


#Outer join of Apple and Spotify stats

joined_tracks = BigQueryOperator(
  task_id='joined_tracks',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/joined_tracks.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.tracks_joined',
  dag = dag
    )


#Gets Spotify metadata

meta_spotify = BigQueryOperator(
  task_id='meta_spotify',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/spotify_metadata.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.spotify_metadata',
  dag = dag
    )


#Gets Apple and some R2 metadata

meta_apple = BigQueryOperator(
  task_id='meta_apple',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/apple_metadata.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.apple_r2_metadata',
  dag = dag
    )


#Gets all the above stats together

data1 = BigQueryOperator(
  task_id='data1',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_selecta_data1.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.data_iteration1',
  dag = dag
    )


#Gets the count of each isrc in the fixed playlists

fp_isrc_count = BigQueryOperator(
  task_id='fp_isrc_count',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_isrc_count.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.fp_isrc_count',
  dag = dag
    )



#Stat if isrc is in recommender model

in_recommender = BigQueryOperator(
  task_id='in_recommender',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/in_recommender.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.in_recommender',
  dag = dag
    )



#Getting quantiles and usage count

quantiles_counts = BigQueryOperator(
  task_id='quantiles_counts',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_selecta_data2.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.data_iteration2',
  dag = dag
    )

#Final gettogether

data_final = BigQueryOperator(
  task_id='data_final',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_data_full.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_selecta.final_data',
  dag = dag
    )

#Task dependencies

tracks_spotify >> joined_tracks
tracks_apple >> joined_tracks
joined_tracks >> data1
meta_spotify >> data1
meta_apple >> data1
data1 >> fp_isrc_count
data1 >> in_recommender
data1 >> quantiles_counts
fp_isrc_count >> data_final
in_recommender >> data_final
quantiles_counts >> data_final
