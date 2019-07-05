import airflow
import os
import pytz
from airflow.models import BaseOperator
from datetime import date, timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

start_date = datetime(2019, 5, 31)

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


dag = DAG('storechecker_dashboard',
  description = 'Data gathering for Applemusic Storechecker dashboard',
  schedule_interval='45 12 * * 5',
  max_active_runs=1,
  default_args=default_args)


#Gets the list of Spotify ISRCs, users and listens for the last month

combine_applemusic = BigQueryOperator(
  task_id='combine_applemusic',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/combine_applemusic.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.store_checker.all_applemusic',
  dag = dag)
