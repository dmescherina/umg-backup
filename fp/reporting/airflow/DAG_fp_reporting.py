import airflow
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_download_operator
from airflow.operators.bash_operator import BashOperator
import airflow.macros

import pytz
from datetime import date, timedelta, datetime

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('fixed_playlists_reporting',
  description = 'Consumption and Streaming data generation for Fixed Playlists',
  schedule_interval='30 9 * * *', # run daily at 9:30am
  max_active_runs=1,
  default_args=args)

## Getting consumption data

consumption_data = BigQueryOperator(
  task_id='consumption_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/consumption_data.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.consumption',
  dag = dag
  )

## Getting Spotify streaming data

spotify_data = BigQueryOperator(
  task_id='spotify_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_spotify.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming',
  dag = dag
  )

## Getting Apple streaming data

apple_data = BigQueryOperator(
  task_id='apple_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_apple.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming',
  dag = dag
  )

## Getting Deezer streaming data

deezer_data = BigQueryOperator(
  task_id='deezer_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_deezer.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming',
  dag = dag
  )

## Getting Amazon Prime streaming data

amazon_prime_data = BigQueryOperator(
  task_id='amazon_prime_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_amazon_prime.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming',
  dag = dag
  )

## Getting Amazon Unlimited streaming data

amazon_unlimited_data = BigQueryOperator(
  task_id='amazon_unlimited_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/fp_amazon_unlimited.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming',
  dag = dag
  )

## Adding metadata

metadata = BigQueryOperator(
  task_id='metadata',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/streaming_with_meta.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.streaming_with_meta',
  dag = dag
  )

## Check if playlists are delivered to all partners

check_playlists_partners = BigQueryOperator(
  task_id='check_playlists_partners',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/check_playlists_partners.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists_data.check_playlists_partners',
  dag = dag
  )

## Getting playlists table into a file

playlists_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='playlists_to_gcs',
    source_project_dataset_table='umg-comm-tech-dev.fixed_playlists_data.playlists_list',
    destination_cloud_storage_uris=['gs://umg-comm-tech-dev/data/fixed/playlists_list.csv'],
    export_format='CSV',
    field_delimiter='\t',
    print_header=True,
    bigquery_conn_id='bigquery_default',
    dag=dag)


download_from_gcs = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_from_gcs',
    bucket='umg-comm-tech-dev',
    object= 'data/fixed/playlists_list.csv',
    filename='/home/airflow/gcs/data/fixed/playlists_list.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag)

download_from_gcs2 = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_from_gcs2',
    bucket='umg-comm-tech-dev',
    object= 'qubole/user-data/pitched/fixed/prod/compilations-{{macros.ds_format(macros.ds_add( ds, 0),\'%Y-%m-%d\',\'%Y-%m-%d\')}}.csv',
    filename='/home/airflow/gcs/data/fixed/compilations.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag)

## Concatenating new and old playlists

concat_playlists = BashOperator(
        task_id ='concat_playlists',
        bash_command='python /home/airflow/gcs/dags/app/concat_playlists.py /home/airflow/gcs/data/fixed/playlists_list.csv /home/airflow/gcs/data/fixed/compilations.csv /home/airflow/gcs/data/fixed/all_playlists.csv' ,
        dag=dag)

load_playlists_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_playlists_bq',
    bucket='us-central1-comm-tech-flow--c62114c2-bucket',
    source_objects=['data/fixed/all_playlists.csv'],
    destination_project_dataset_table='umg-comm-tech-dev.fixed_playlists_data.playlists_list',
    schema_fields=[
  {
    "description": "artists",
    "mode": "NULLABLE",
    "name": "artists",
    "type": "STRING"
  },
  {
    "description": "genre",
    "mode": "NULLABLE",
    "name": "genre",
    "type": "STRING"
  },
  {
    "description": "playlist_owner",
    "mode": "NULLABLE",
    "name": "playlist_owner",
    "type": "STRING"
  },
  {
    "description": "release_date",
    "mode": "NULLABLE",
    "name": "release_date",
    "type": "TIMESTAMP"
  },
  {
    "description": "release_title",
    "mode": "NULLABLE",
    "name": "release_title",
    "type": "STRING"
  },
  {
    "description": "repertoire_pools",
    "mode": "NULLABLE",
    "name": "repertoire_pools",
    "type": "STRING"
  },
    {
    "description": "status",
    "mode": "NULLABLE",
    "name": "status",
    "type": "STRING"
  },
    {
    "description": "streaming_services",
    "mode": "NULLABLE",
    "name": "streaming_services",
    "type": "STRING"
  },
    {
    "description": "take_down_date",
    "mode": "NULLABLE",
    "name": "take_down_date",
    "type": "TIMESTAMP"
  },
    {
    "description": "territory",
    "mode": "NULLABLE",
    "name": "territory",
    "type": "STRING"
  },
    {
    "description": "upc",
    "mode": "NULLABLE",
    "name": "upc",
    "type": "STRING"
  }
],
    skip_leading_rows=1,
    source_format='csv',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='bigquery_default',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag)


#Task dependencies

playlists_to_gcs >> download_from_gcs >> concat_playlists >> load_playlists_bq >> spotify_data
load_playlists_bq >> apple_data
load_playlists_bq >> deezer_data
load_playlists_bq >> amazon_prime_data
load_playlists_bq >> amazon_unlimited_data
download_from_gcs2 >> concat_playlists
spotify_data >> metadata
apple_data >> metadata
deezer_data >> metadata
amazon_prime_data >> metadata
amazon_unlimited_data >> metadata
consumption_data >> check_playlists_partners
metadata >> check_playlists_partners
