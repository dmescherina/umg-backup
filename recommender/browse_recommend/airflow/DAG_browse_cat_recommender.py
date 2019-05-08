import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators import gcs_download_operator
from airflow.contrib.operators import bigquery_to_gcs
import airflow.macros

import pickle
import pytz
from datetime import date, timedelta, datetime

#import pandas as pd
from pandas.io import gbq

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 15),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('browse_categories_recommendations_prerun',
  description = 'Preprocessing recommendations for territories and their browse categories most local and relevant playlists',
  schedule_interval='0 0 5 * *', # run on 5th of every month - for testing purposes
  max_active_runs=1,
  default_args=args)


### Run the query to get the target playlists for each territory and browse category

gather_playlists = BigQueryOperator(
  task_id='gather_playlists',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_browse_playlists.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.browse_recommender.target_browse_playlists',
  dag = dag
  )

### Get the data for our playlists using Christine's algorithm

pl_listen_stats = BigQueryOperator(
  task_id='pl_listen_stats',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_playlist_stats.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.browse_recommender.playlist_data',
  dag = dag
  )

playlists_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='playlists_to_gcs',
    source_project_dataset_table='umg-comm-tech-dev.browse_recommender.target_browse_playlists',
    destination_cloud_storage_uris=['gs://umg-comm-tech-dev/recommender/browse_recommendations/playlists_list.csv'],
    export_format='CSV',
    field_delimiter=',',
    print_header=True,
    bigquery_conn_id='bigquery_default',
    dag=dag
)

stats_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='stats_to_gcs',
    source_project_dataset_table='umg-comm-tech-dev.browse_recommender.playlist_data',
    destination_cloud_storage_uris=['gs://umg-comm-tech-dev/recommender/browse_recommendations/playlists_data.csv'],
    export_format='CSV',
    field_delimiter=',',
    print_header=True,
    bigquery_conn_id='bigquery_default',
    dag=dag
)

download_playlists = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_playlists',
    bucket='umg-comm-tech-dev',
    object= 'recommender/browse_recommendations/playlists_list.csv',
    filename='/home/airflow/gcs/data/playlists_list.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag

)

download_data = gcs_download_operator.GoogleCloudStorageDownloadOperator(
    task_id='download_data',
    bucket='umg-comm-tech-dev',
    object= 'recommender/browse_recommendations/playlists_data.csv',
    filename='/home/airflow/gcs/data/playlists_data.csv',
    bigquery_conn_id='google_cloud_storage_default',
    dag=dag

)

# download_meta_dict = gcs_download_operator.GoogleCloudStorageDownloadOperator(
#     task_id='download_meta_dict',
#     bucket='umg-comm-tech-dev',
#     object= 'recommender/metadata/metadata_dict.pkl',
#     filename='/home/airflow/gcs/data/metadata_dict.pkl',
#     bigquery_conn_id='google_cloud_storage_default',
#     dag=dag

# )

#scipy_install = BashOperator(
#        task_id='scipy_install',
#        bash_command="sudo apt-get install python-scipy",
#        dag = dag
#)

### Get no more than first 10 for each territory-category combination
get_browse_playlists = BashOperator(
        task_id ='get_browse_playlists',
        bash_command='python /home/airflow/gcs/dags/app/browse_recommend.py /home/airflow/gcs/data/playlists_list.csv /home/airflow/gcs/data/playlists_data.csv /home/airflow/gcs/data/browse_playlists.csv',
        dag=dag
  )

### Load the model

#download_model = gcs_download_operator.GoogleCloudStorageDownloadOperator(
#    task_id='download_model',
#    bucket='umg-comm-tech-dev',
#    object='recommender/model_20181112.pkl',
#    filename='/home/airflow/gcs/dags/app/model.pkl',
#    bigquery_conn_id='google_cloud_storage_default',
#    dag=dag
#)

#install_scipy = BashOperator(
#        task_id ='install_scipy',
#        bash_command='python /home/airflow/gcs/dags/app/install_scipy.py',
#        dag=dag
#  )

### Update Pandas to the latest version
update_pandas = BashOperator(
        task_id ='update_pandas',
        bash_command='sudo pip3 install --upgrade pandas',
        dag=dag
  )

### Produce recommendations, concatenate them together and save them as a CSV in the bucket
get_recommendations = BashOperator(
        task_id ='get_recommendations',
        bash_command='python /home/airflow/gcs/dags/app/get_recommendations.py /home/airflow/gcs/data/browse_playlists.csv /home/airflow/gcs/data/browse_recommendations.csv',
        dag=dag
  )

transfer_output = BashOperator(
        task_id='transfer_output',
        bash_command="gsutil cp /home/airflow/gcs/data/browse_recommendations.csv  gs://umg-comm-tech-dev/recommender/browse_recommendations/browse_recommendations_{{macros.ds_format(macros.ds_add( ds, 30),\'%Y-%m-%d\',\'%Y%m%d\')}}.csv",
        dag = dag
)

remove_files_worker = BashOperator(
        task_id='remove_files_worker',
        bash_command="rm -f /home/airflow/gcs/data/browse_recommendations.csv /home/airflow/gcs/data/playlists_list.csv /home/airflow/gcs/data/playlists_data.csv /home/airflow/gcs/data/browse_playlists.csv",
        dag = dag
)

### Task dependencies
gather_playlists >> pl_listen_stats >> playlists_to_gcs >> stats_to_gcs >> download_playlists >> download_data >> get_browse_playlists >> update_pandas >> get_recommendations >> transfer_output >> remove_files_worker

