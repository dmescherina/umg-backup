from __future__ import print_function
import airflow
import os
import pytz
from airflow.models import BaseOperator
from datetime import date, timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_download_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
import airflow.macros
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataproc_operator import DataProcHadoopOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
import pandas as pd
import csv
import glob
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryValueCheckOperator
from airflow.hooks import S3Hook



start_date = datetime(2018, 06, 01 , 0, 0, 0, tzinfo=pytz.utc)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None ,
    'email': ['daniel.usvyat@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)

}


dag = DAG('Pitched_optimize',
  description = 'PROD version of PITCHED Optimize data creation',
  schedule_interval='0 3 * * *',
  max_active_runs=1,
  default_args=default_args)
'''
slack_token = os.environ["xoxp-25870709394-243585439158-289882853395-ef2efd07cdef587adff296bbcaa67485"]

sc = SlackClient(slack_token)

def slack_failed_task(contextDictionary, **kwargs):
    base_url = mybasedUrl
    task_instance = contextDictionary.get('task_instance')
    log_url = '{task_instance.log_url}'.format(**locals())
    failed_alert = sc.api_call('chat.postMessage',
    link_names=1,
    task_id='slack_failed',
    channel='#apollodev',
    icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
    username='daniel.usvyat',
    text = ':hankey: DAG Failed on dagid=*{task_instance.dag_id}* taskid=*{task_instance.task_id}* :page_facing_up: <{log_url}|see log> @mehdio :trollface: ? '.format(**locals()))
    return failed_alert.execute
'''

#load playlist uri look up list for optimize from the GCS bucket to Big Query

load_to_uri_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_uri_bq',
    bucket='umg-comm-tech-dev',
    source_objects=['qubole/user-data/alan/apollo/lookups/uris/umg_ba.ar_apollo_uri_lkup'],
    destination_project_dataset_table='umg-comm-tech-dev.Optimize.ar_apollo_uri_lkup',
    schema_fields=[
  {
    "description": "playlist Uri",
    "mode": "NULLABLE",
    "name": "uri",
    "type": "STRING"
  }],
    skip_leading_rows=1,
    source_format='csv',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='bigquery_default',
    #on_failure_callback=slack_failed_task,
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)

#load playlist owner look up list for optimize from the GCS bucket to Big Query
load_to_owner_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_owner_bq',
    bucket='umg-comm-tech-dev',
    source_objects=['qubole/user-data/alan/apollo/lookups/owners/Spotify_Owner_Lookup.csv'],
    destination_project_dataset_table='umg-comm-tech-dev.Optimize.ar_apollo_owner_lkup',
    schema_fields=[
  {
    "description": "owner id",
    "mode": "NULLABLE",
    "name": "owner_id",
    "type": "STRING"
  },
  {
    "description": "major type",
    "mode": "NULLABLE",
    "name": "majortype",
    "type": "STRING"
  },
  {
    "description": "minor type",
    "mode": "NULLABLE",
    "name": "minortype",
    "type": "STRING"
  },
  {
    "description": "desc",
    "mode": "NULLABLE",
    "name": "desc",
    "type": "STRING"
  },
  {
    "description": "desc2",
    "mode": "NULLABLE",
    "name": "desc2",
    "type": "STRING"
  },
  {
    "description": "desc3",
    "mode": "NULLABLE",
    "name": "desc3",
    "type": "STRING"
  }
],
    skip_leading_rows=1,
    source_format='csv',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='bigquery_default',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)



#check count of rows in streams table for execution date is above threshold

check_streams = BigQueryCheckOperator(
    task_id = 'check_streams',
    sql = '''

select count(*)

from [umg-partner:spotify.streams] p
where _PARTITIONTIME = timestamp('{{ macros.ds_add( ds, -1) }}')
having count(*) > 573319306


''',
    bigquery_conn_id='bigquery_default',
    dag = dag

    )

#check count of rows in playlist track history table for execution date is above threshold

check_playlist_track_history = BigQueryCheckOperator(
    task_id = 'check_playlist_track_history',
    sql = '''

select count(*)

from [umg-partner:spotify.playlist_track_history] p
where _PARTITIONTIME = timestamp('{{ macros.ds_add( ds, -1) }}')
having count(*) > 13075743
''',
    bigquery_conn_id='bigquery_default',
    dag = dag

    )

#builds list of playlist uris and meta data for Pitched Optimize playlists

bq_1 = BigQueryOperator(
  task_id='ar_tpr_seed_uris',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_ar_tpr_seed_uris.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.ar_tpr_seed_uris',
  dag = dag
    )

#shows user clusters of users who listen to each of the playlists that are being monitored on Pitched

bq_2 = BigQueryOperator(
  task_id='ar_tpe_playlistclusters_base',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_ar_tpe_playlistclusters_base.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.ar_tpe_playlistclusters_base',
  dag = dag

   )

"""
shows user clusters per playlist, showing the percentage of users listening to a playlist
are from each of the cluster as well as what each represented cluster represents the precentage of global user base
"""

bq_3 = BigQueryOperator(
  task_id='ar_tpe_playlistclusters_base_perc',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_ar_tpe_playlistclusters_base_perc.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.ar_tpe_playlistclusters_base_perc',
  dag = dag
    )

"""
look up table for all tracks streamed on a given execution date ranked by streams on a track level
by streams and grouped by partner track id and stream durations
"""

bq_4 = BigQueryOperator(
  task_id='AR_TPR_TrackSkipLkup',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_TrackSkipLkup.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_TrackSkipLkup',
  dag = dag
    )

"""
pulls back all the streams at a user level for past 8 weeks for users who have listened
to any tracks on optimize playlists within the 'ar_tpr_seed_uris' look up table
including track metadata, stream souce,
stream duration, stream country,
stream date and rank partitioned by user and
track ordered by stream datetime

This query forms the first step of the discovery metric

"""

bq_5 = BigQueryOperator(
  task_id='AR_TPR_StreamExport',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_StreamExport.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_StreamExport',
  dag = dag
    )

"""
first step to calculating the collection converstion score
"""


bq_6 = BigQueryOperator(
  task_id='AR_TPR_UserDiscCol',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_UserDiscCol.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_UserDiscCol',
  dag = dag
    )

"""
shows the first stream date for a user for each of the optimize
playlists as well as the average daily streams
"""

bq_7 = BigQueryOperator(
  task_id='AR_TPR_UserReturning',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_UserReturning.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_UserReturning',
  dag = dag
    )

"""
calculates skips after threshold as well as average stream length,
calculates number of collection streams, discover streams
and super fan streams orginatiing from an Optmize playlist
"""

bq_8 = BigQueryOperator(
  task_id='AR_TPR_StreamBuild1a',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_StreamBuild1a.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild1a',
  dag = dag
    )


"""
this query fixes missing owner in uri issue caused by GDPR related to how Spotify
reports the playlist uri without the owner
"""

AR_TPR_StreamBuild1b = BigQueryOperator(
  task_id='AR_TPR_StreamBuild1b',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_StreamBuild1b.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild1b',
  dag = dag
    )

"""
summarises what's reported in Streambuild1b
"""

bq_9 = BigQueryOperator(
  task_id='AR_TPR_StreamBuild3',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_StreamBuild3.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild3',
  dag = dag
    )

"""
filters out anything with streams less than 10% of average streams
"""

bq_10 = BigQueryOperator(
  task_id='AR_TPR_StreamBuild3b',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_StreamBuild3b.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_StreamBuild3b',
  dag = dag
    )

"""
calculates whether a track is streamed in head or tail, i.e. is the user listening to the
playlist on shuffle or from the start
"""

bq_11 = BigQueryOperator(
  task_id='AR_TPR_HeadTail_Build2',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_TPR_HeadTail_Build2.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_TPR_HeadTail_Build2',
  dag = dag
    )

"""
calculates average natual log of the order in which tracks are streamed on a playlist and average daily streams,
"""

bq_12 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_mean_estimates',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_mean_estimates.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_mean_estimates',
  dag = dag
    )


"""
calculates the standard deviation  natrual log of the order in which tracks are streamed
on a playlist (x- axis) as well as the standard deviation of daily streams (y-axis)
"""

bq_13 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_stdev_estimates',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_stdev_estimates.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_stdev_estimates',
  dag = dag
    )


"""
rescale mean deviation for x axis and y axis by dividing the difference between observed and their respective average by their respective standard deviations
"""

bq_14 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_standardized_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_standardized_data.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_standardized_data',
  dag = dag
    )

"""
calculate the variance for x axis and y-axis
"""


bq_15 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_standardized_beta_estimates',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_standardized_beta_estimates.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_standardized_beta_estimates',
  dag = dag
    )

"""
calculate alpha (stream performance measure)
and beta measures (stream volatility measure) for the slope
"""

bq_16 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_Slope',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_Slope.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Slope',
  dag = dag
    )


"""
calculates error by subtracking
(alpha + beta * natural log of track position that track stream begun on playlist)
from streams
"""


bq_17 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_find',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_find.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_find',
  dag = dag
    )

"""
calculates the average change point as well as the average error at a playlist level
"""

bq_18 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_Results1',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_Results1.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Results1',
  dag = dag
    )

"""
summarises changepoint and error by whether a playlist is streamed from start
or on shuffle
"""

bq_19 = BigQueryOperator(
  task_id='Ar_TPR_HeadTail_Results2',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_HeadTail_Results2.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_HeadTail_Results2',
  dag = dag
    )

"""
brings in track level info from 'AR_TPR_StreamBuild3b' table and converts discovery,
collection,returning users and super fans to a fraction of streams
calculates httype - whether user streams from start or from shuffle/after head
calculates baseline which is natural log of position + beta and alpha depending on
whether it is head or tail different alpha and betas are added
"""

bq_20 = BigQueryOperator(
  task_id='Ar_TPR_Streams_Final1_Days',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_Streams_Final1_Days.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days',
  dag = dag
    )

"""
calculates:
skips percentage by looking at skips as a fraction streams
skips performance by dividing skips percentage divided by average skips
length peformance by diviving stream duration by average stream duration
collection performance by dividing collection streams by average collection
returning performance by dividing returning streams by average returning streams
super user performance by dividing super user streams by average super user streams
skipsb4 is calculated by dividing streams by baseline
skipsb4 performance is calculated by dividing streams by average skips b4
"""

bq_21 = BigQueryOperator(
  task_id='Ar_TPR_Streams_Final1_Days_Stats',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_Streams_Final1_Days_Stats.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_Stats',
  dag = dag
    )

"""
Some of the calculations from 'Ar_TPR_Streams_Final1_Days_Stats' table are altered:
skips performance by subtracking average skips from skips percentage
length peformance by length subtract average length
collection performance by subtracting average collection from collection
returning performance by subtracting average return streams from return streams
super user performance by substracting average super user streams from super user streams
skipsb4 is calculated by dividing streams by baseline
skipsb4 performance by subtracting average skips b4 threshold from skipsb4

"""

bq_22 = BigQueryOperator(
  task_id='Ar_TPR_Streams_Final1_Days_stats2',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_Streams_Final1_Days_stats2.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_stats2',
  dag = dag
    )

"""
Has the aforementioned metrics along with:
skipsperfbench - subtracting a polynomial depending on the user cluster from skips performance
colperfbench - subtracting a polynomial depending on the user cluster from colperf
"""


bq_23 = BigQueryOperator(
  task_id='Ar_TPR_Streams_Final1_Days_stats2_Bench',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_Ar_TPR_Streams_Final1_Days_stats2_Bench.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.Ar_TPR_Streams_Final1_Days_stats2_Bench',
  dag = dag
    )

"""
selects everything from 'Ar_TPR_Streams_Final1_Days_stats2_Bench'
where stream date is equal to dag execution date
"""

bq_24 = BigQueryOperator(
  task_id='ar_tpr_final_data_store',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_ar_tpr_final_data_store.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.ar_tpr_final_data_store',
  dag = dag
    )

"""
pulls in all the metrics from 'ar_tpr_final_data_store' along with labelling the userclusters
extracts release year from the isrc
determines whether content is frontline, catalogue or carry over. This is currently hard coded in sql:
frontline = 2018
carryover = 2017
catalogue = any other year
pulls in major, minor and sub label types
playlist owner
discovery type:
discovery less than 0.3  = hits
discovery less than 0.5 = develops
discovery is anything else = developing
perfcode is a combination of either high or low (i.e HHH or LLL etc.) for skipsperfbench, colperfbench and skipsb4
when skipsperfbench, colperfbench and skipsb4 less than zero = "L" otherwise = "H"
Perftype  calculated as follows:

Average is sqrt(skipsperfbench^2 + colperfbench^2)

Passive reaction is when skipsperfbench > 0 AND colperfbench < 0 AND skipsb4 > 0

Poor Performer is when skipsperfbench > 0 AND colperfbench < 0 AND skipsb4 < 0

Polarizing Reaction is when skipsperfbench > 0 AND colperfbench > 0 AND skipsb4 > 0

"""

bq_25 = BigQueryOperator(
  task_id='AR_APOLLO_DATA_load',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_APOLLO_DATA_load.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_APOLLO_DATA_load',
  dag = dag
    )

"""
selects all columns from 'ar_tpr_final_data_store' along with some
final cosmetic edits on some of the string columns
"""


bq_26 = BigQueryOperator(
  task_id='AR_APOLLO_DATA_prod',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_APOLLO_DATA_prod.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_APOLLO_DATA_prod',
  dag = dag
    )


"""
same as 'AR_APOLLO_DATA_load' expect for competitor tracks
"""

AR_APOLLO_DATA_load_comp = BigQueryOperator(
  task_id='AR_APOLLO_DATA_load_comp',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_APOLLO_DATA_load_comp.sql',
write_disposition='WRITE_TRUNCATE',
destination_dataset_table = 'umg-comm-tech-dev.Optimize.AR_APOLLO_DATA_load_comp',
dag=dag

)

"""
same as 'AR_APOLLO_DATA_prod' expect for competitor tracks
"""

AR_APOLLO_DATA_prod_non_umg  = BigQueryOperator(
  task_id='AR_APOLLO_DATA_prod_non_umg',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/prod_AR_APOLLO_DATA_prod_non_umg.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.Optimize.non_umg_tracks_prod',
  dag = dag
    )


"""
 exports output from 'AR_APOLLO_DATA_prod to GCS bucket
"""

export_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_to_gcs',
    source_project_dataset_table='umg-comm-tech-dev.Optimize.AR_APOLLO_DATA_prod',
    destination_cloud_storage_uris=['gs://umg-comm-tech-dev/data/apollo/prod/date_id={{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}/apollo_export_{{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}.csv'],
    export_format='CSV',
    field_delimiter='\t',
    print_header=False,
    bigquery_conn_id='bigquery_default',
    dag=dag
)

"""
exports output from 'AR_APOLLO_DATA_prod_non_umg to GCS bucket
"""

export_to_gcs_non_umg = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_to_gcs_non_umg',
    source_project_dataset_table='umg-comm-tech-dev.Optimize.non_umg_tracks_prod',
    destination_cloud_storage_uris=['gs://umg-comm-tech-dev/data/apollo/prod/date_id={{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}/apollo_export_non_umg_{{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}.csv'],
    export_format='CSV',
    field_delimiter='\t',
    print_header=False,
    bigquery_conn_id='bigquery_default',
    dag=dag
)

"""
downloads prod umg file from GCS to worker
"""

download_from_gcs = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_from_gcs',
    bucket='umg-comm-tech-dev',
    object= 'data/apollo/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}/apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
    filename='/home/airflow/gcs/data/apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag

)

"""
downloads non-umg file from GCS to worker
"""

download_from_gcs_non_umg = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_from_gcs_non_umg',
    bucket='umg-comm-tech-dev',
    object='data/apollo/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}/apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
    filename='/home/airflow/gcs/data/apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag

)

"""
runs a python script to fill nulls for umg prod file
"""

fill_nulls = BashOperator(
        task_id ='fill_nulls',
        bash_command='python /home/airflow/gcs/dags/app/prod_fill_na.py /home/airflow/gcs/data/apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv /home/airflow/gcs/data/apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}' ,
        dag=dag
  )

"""
runs a python script to fill nulls for non-umg prod file
"""

fill_nulls_non_umg = BashOperator(
        task_id ='fill_nulls_non_umg',
        bash_command='python /home/airflow/gcs/dags/app/prod_fill_na.py /home/airflow/gcs/data/apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv /home/airflow/gcs/data/apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}' ,
        dag=dag
  )

"""
ensures lzop compression package is installed on the worker
"""

lzop_install = BashOperator(
        task_id='lzop_install',
        bash_command="sudo apt-get install -y lzop",
        dag = dag
)

"""
lzo compresses the umg prod file
"""


lzo_convert = BashOperator(
        task_id='lzo_convert',
        bash_command="lzop -1 /home/airflow/gcs/data/apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}",
        dag = dag
)

"""
lzo compresses the non-umg prod file
"""

lzo_convert_non_umg= BashOperator(
        task_id='lzo_convert_non_umg',
        bash_command="lzop -1 /home/airflow/gcs/data/apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}",
        dag = dag
)

"""
uploads lzo files back to GCS
"""

lzo_upload = BashOperator(
        task_id='lzo_upload',
        bash_command="gsutil cp /home/airflow/gcs/data/apollo_export_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.lzo  gs://umg-comm-tech-dev/data/apollo/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}/",
        dag = dag
)

"""
removes files on worker
"""

remove_files_worker = BashOperator(
        task_id='remove_files_worker',
        bash_command="rm -f /home/airflow/gcs/data/*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.lzo /home/airflow/gcs/data/apollo_export_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.csv /home/airflow/gcs/data/apollo_export_non_umg_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.csv /home/airflow/gcs/data/apollo_export_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}* /home/airflow/gcs/data/apollo_export_non_umg_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*",
        dag = dag
)


"""
removes original csv files from GCS
"""

remove_csv_gcs = BashOperator(
        task_id='remove_files_gcs',
        bash_command="gsutil rm gs://umg-comm-tech-dev/data/apollo/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}/*apollo_export_*{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.csv",
        dag = dag
)

"""
checks if folder size for execution date is abover threshold
"""

check_gcs_size = BashOperator(
        task_id='check_gcs_size',
        bash_command="python /home/airflow/gcs/dags/app/prod_check_gcs_size.py {{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}",
        dag = dag
)


"""
exports lzo files to s3 bucket
"""

def uploadToS3(**kwargs):
    temp = kwargs.get('templates_dict', {})
    bucket = temp.get('bucket')
    key = temp.get('key')
    local_dir = temp.get('local_dir')
    files = temp.get('files')

    s3 = S3Hook()
    for file in files:
        print("Loading file: ", file)
        try:
            s3.load_file(
            local_dir + '/' + file,
            key + '/' + file,
            bucket,
            replace=True)
            print("Files successfully loaded into S3")
        except Exception as e:
            print(e.__doc__)
            print(e.message)

export_to_s3 = PythonOperator(
    task_id='export_to_s3',
    python_callable=uploadToS3,
    provide_context=True,
    templates_dict={
        'bucket': 'umg-ers-analytics',
        'key': 'qubole/user-data/pitched/optimize/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
        'local_dir': '/home/airflow/gcs/data',
        'files': [
          'apollo_export_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.lzo',
          'apollo_export_non_umg_{{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}.lzo'],
    },
    dag=dag
)

# export_to_s3 = BashOperator(
#             task_id='export_to_s3',
#             bash_command="gsutil cp -r gs://umg-comm-tech-dev/data/apollo/prod/date_id={{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}} s3://umg-ers-analytics/qubole/user-data/pitched/optimize/prod",
#             dag = dag
# )

"""
task dependencies for dag
"""

load_to_owner_bq >> bq_1
load_to_uri_bq >> bq_1
check_streams >> bq_1
check_playlist_track_history >> bq_1
bq_1 >> bq_2 >> bq_3 >> bq_23
bq_1 >> bq_5 >> bq_6
bq_4 >> bq_9
bq_5 >> bq_7 >> bq_8
bq_6 >> bq_8
bq_8 >> AR_TPR_StreamBuild1b
AR_TPR_StreamBuild1b >>  bq_9 >>  bq_10 >> bq_11
bq_11 >>  bq_12 >> bq_13 >> bq_14 >> bq_15 >> bq_16
bq_16 >> bq_17 >> bq_18 >> bq_19 >> bq_20 >> bq_21
bq_21 >> bq_22 >> bq_23 >> bq_24 >> bq_25 >> bq_26
bq_24 >> AR_APOLLO_DATA_load_comp >> AR_APOLLO_DATA_prod_non_umg
bq_26 >> export_to_gcs >> download_from_gcs >> lzop_install >> fill_nulls >> lzo_convert >> lzo_upload
AR_APOLLO_DATA_prod_non_umg >> export_to_gcs_non_umg >> download_from_gcs_non_umg >> lzop_install >> fill_nulls_non_umg >> lzo_convert_non_umg >> lzo_upload
#lzo_upload_non_umg >> remove_csv_gcs
lzo_upload >> remove_csv_gcs >> check_gcs_size >> export_to_s3 >> remove_files_worker
#lzo_upload >> remove_csv_gcs >> remove_files_worker
