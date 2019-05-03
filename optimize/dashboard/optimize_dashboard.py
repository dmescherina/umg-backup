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
from airflow.contrib.operators.bigquery_check_operator import BigQueryValueCheckOperator
import pandas as pd
import csv
import glob




start_date = datetime(2018, 01, 01 , 0, 0, 0, tzinfo=pytz.utc)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None ,
    'email': ['daniel.usvyat@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)

}


dag = DAG('Pitched_optimize_dashboard',
  description = 'refresh views for optimize dashboard in data studios',
  schedule_interval='0 10 * * *',
  max_active_runs=25,
  default_args=default_args,
  catchup = True)


#check streams & playlist_track_history tables


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




#builds up stats on streams for optimized UMG owned playlists

optimized_playlist_uris = BigQueryOperator(
  task_id='optimized_playlist_uris',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/optimized_uris.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.optimized_uris',
  dag = dag
    )


optimized_playlist_stats = BigQueryOperator(
  task_id='optimized_playlist_stats',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/optimized_playlist_view.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.optimize_playlist_stats${{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
  dag = dag
    )


optimized_owner_rankings = BigQueryOperator(
  task_id='optimized_owner_rankings',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/optimize_userlevel_stats.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.optimized_owner_rankings',
  dag = dag
    )

#builds up stats on streams for non-optimized UMG owned playlists

non_optimized_playlist_stats = BigQueryOperator(
  task_id='non_optimized_playlist_stats',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/non_optimized_playlist_view.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.non_optimize_playlist_stats${{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
  dag = dag
    )

major_usage = BigQueryOperator(
  task_id='major_usage',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/major_usage.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.usage_owner',
  dag = dag
    )

unite_stats = BigQueryOperator(
  task_id='unite_stats',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/union.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.optimize_reporting.all_playlist_stats${{macros.ds_format(macros.ds_add( ds, -1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
  dag = dag
    )

check_playlist_track_history >> optimized_playlist_uris
check_playlist_track_history >> non_optimized_playlist_stats
check_playlist_track_history >> major_usage

check_streams >> optimized_playlist_uris >> optimized_owner_rankings
check_streams >> non_optimized_playlist_stats
check_streams >> major_usage

optimized_playlist_uris >> optimized_playlist_stats
optimized_playlist_uris >> non_optimized_playlist_stats
non_optimized_playlist_stats >> unite_stats
optimized_playlist_stats >> unite_stats
