from __future__ import print_function
from slackclient import SlackClient
import airflow
import os
import pytz
from airflow.models import BaseOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_download_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
import airflow.macros
from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataproc_operator import DataProcHadoopOperator
import pandas as pd
import csv
import glob



start_date = datetime(2018, 9, 17, 0, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': "@daily" ,
    'email': ['fred.foos@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)

}


dag = DAG('itunes_store_checker_aunz',
  description = 'pulling itunes storechecker data into BQ for AUNZ region',
  schedule_interval='00 01 * * *',
  max_active_runs=3,
  default_args=default_args)

#gcs_sensor_itunes_aunz = GoogleCloudStorageObjectSensor(
#    task_id='gcs_sensor_itunes_aunz',
#    bucket='umg-comm-tech-dev',
#    object='/apple_music_tool_reports/AUNZ/ITUNESDESKTOP-AU-{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
#    google_cloud_conn_id='google_cloud_storage_default',
#    dag=dag)

make_dir_worker = BashOperator(
        task_id='make_dir_worker',
        bash_command='mkdir -m 777 -p /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
        dag=dag
        )

download_from_gcs = BashOperator(
        task_id='download_from_gcs',
        bash_command='gsutil cp gs://umg-comm-tech-dev/apple_music_tool_reports/AUNZ/*ITUNESDESKTOP*{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.csv /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/',
        dag=dag
        )



join_csv_itunes = BashOperator(
        task_id='join_csv_itunes',
        bash_command='python /home/airflow/gcs/dags/app/itunes_join_files_v3.py /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/ *ITUNESDESKTOP*{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}*.csv /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/ITUNES{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv',
        dag=dag
        )

upload_joined_files = BashOperator(
        task_id='upload_joined_files',
        bash_command='gsutil cp /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/ITUNES{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv gs://umg-comm-tech-dev/store_checker/AUNZ/date_id={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/',
        dag=dag
        )

load_to_itunes_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_itunes_bq',
    bucket='umg-comm-tech-dev',
    source_objects=['store_checker/AUNZ/date_id={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/ITUNES{{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv'],
    destination_project_dataset_table='umg-comm-tech-dev.store_checker.itunes${{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
    skip_leading_rows=0,
    source_format='csv',
    field_delimiter=',',
    allow_jagged_rows=True,
    allow_quoted_newlines=True,
    write_disposition='WRITE_APPEND',
    bigquery_conn_id='bigquery_default',
    #on_failure_callback=slack_failed_task,
    schema_fields=[
{'type': 'INTEGER', 'name': 'index', 'mode': 'NULLABLE'},
{'type': 'DATE', 'name': 'Date', 'mode': 'NULLABLE'},
{'description': 'skip', 'type': 'STRING', 'name': 'Crawl_Time', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Country', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Storefront', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Page_Title', 'mode': 'NULLABLE'},
{'type': 'INTEGER', 'name': 'Page_Depth', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Page_Priority', 'mode': 'NULLABLE'},
{'description': 'hidden', 'type': 'STRING', 'name': 'Page_Url', 'mode': 'NULLABLE'},
{'description': 'hidden', 'type': 'STRING', 'name': 'Page_ParentUrl', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Type', 'mode': 'NULLABLE'}, {'type': 'STRING', 'name': 'Section_Title', 'mode': 'NULLABLE'},
{'type': 'INTEGER', 'name': 'Page_Position', 'mode': 'NULLABLE'}, {'type': 'INTEGER', 'name': 'Section_Position', 'mode': 'NULLABLE'},
{'description': 'include', 'type': 'STRING', 'name': 'Title', 'mode': 'NULLABLE'}, {'type': 'STRING', 'name': 'Url', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Image_Url', 'mode': 'NULLABLE'}, {'type': 'STRING', 'name': 'Product_Type', 'mode': 'NULLABLE'},
{'description': 'apple id, needs to be matched with DSC UPC report. be wary of two supply chains (US) - rename to apple_ID', 'type': 'STRING', 'name': 'Product_Id', 'mode': 'NULLABLE'},
{'type': 'STRING', 'name': 'Product_Check_Status', 'mode': 'NULLABLE'},
{'description': 'marry to get label, also include minor label', 'type': 'STRING', 'name': 'Major_Label', 'mode': 'NULLABLE'},
{'description': 'include', 'type': 'STRING', 'name': 'Collection_Id', 'mode': 'NULLABLE'},
{'description': 'include', 'type': 'STRING', 'name': 'Song_Id', 'mode': 'NULLABLE'},
{'description': 'include', 'type': 'STRING', 'name': 'Video_Id', 'mode': 'NULLABLE'},
{'description': 'skip', 'type': 'STRING', 'name': 'Price', 'mode': 'NULLABLE'},
{'description': '', 'type': 'STRING', 'name': 'Artist', 'mode': 'NULLABLE'},
{'description': '', 'type': 'STRING', 'name': 'Title2', 'mode': 'NULLABLE'}],
    google_cloud_storage_conn_id='google_cloud_storage_default',
    dag=dag
)



check_bq_itunes = BigQueryTableSensor(
    task_id='check_bq_itunes',
    project_id = 'umg-comm-tech-dev',
    dataset_id = 'store_checker',
    table_id = 'itunes${{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}',
    bigquery_conn_id='bigquery_default',
    dag = dag
    )


remove_files_worker = BashOperator(
        task_id='remove_files_worker',
        bash_command="rm -f /home/airflow/gcs/data/itunes/AUNZ/date={{macros.ds_format(macros.ds_add( ds, 1),\'%Y-%m-%d\',\'%Y%m%d\')}}/*.csv",
        dag = dag
)


#gcs_sensor_itunes_aunz >>
make_dir_worker >> download_from_gcs >> join_csv_itunes >> upload_joined_files >> load_to_itunes_bq >> check_bq_itunes >> remove_files_worker
