import airflow
import os
import pytz
from airflow.models import BaseOperator
from datetime import date, timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

start_date = datetime(2019, 4, 30, 0, 0, 0, tzinfo=pytz.utc)

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


dag = DAG('financials_create_deezer_streams',
  description = 'Deezer Data for Create playlists for Financials Dashboard',
  schedule_interval='0 7 * * *',
  max_active_runs=1,
  default_args=default_args)


#Gets the list of Create playlists that are also in Deezer data

create_in_deezer = BigQueryOperator(
  task_id='create_in_deezer',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/create_in_deezer.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.deezer.create_in_deezer',
  dag = dag
    )

#Gets the list of Create playlists that are also in Napster

create_in_napster = BigQueryOperator(
  task_id='create_in_napster',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/create_in_napster.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.deezer.create_in_napster',
  dag = dag
    )

#Gets Deezer streams for the above playlists

create_streams = BigQueryOperator(
  task_id='create_streams',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/create_streams.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.deezer.create_streams',
  dag = dag
    )

#Estimates Napster streams using Spotify streams and adjusting for streaming shares

create_streams_napster = BigQueryOperator(
  task_id='create_streams_napster',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/create_streams_napster.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.deezer.create_streams',
  dag = dag
    )


#Task dependencies

create_in_deezer >> create_streams
create_in_napster >> create_streams_napster
