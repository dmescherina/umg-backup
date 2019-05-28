import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import gcs_download_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import airflow.macros

import pytz
from datetime import date, timedelta, datetime

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 5),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('recommender_model_update',
  description = 'Gathering data, model training and metadata dictionary update',
  schedule_interval='0 0 5 * *', # run monthly on 6th
  max_active_runs=1,
  default_args=args)

### Gather playlists uris with more than 10 followers
get_playlists = BigQueryOperator(
  task_id='get_playlists',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_playlists.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.recommender_model.all_playlists',
  dag = dag
  )

## Gather playlists uris from browse category playlists if they're not already in the model
get_missing_browse_playlists = BigQueryOperator(
  task_id='get_missing_browse_playlists',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_missing_browse_playlists.sql',
  write_disposition='WRITE_APPEND',
  destination_dataset_table = 'umg-comm-tech-dev.recommender_model.all_playlists',
  dag = dag
  )

### Get all the isrcs and track metadata for our playlists universe

get_isrc_meta = BigQueryOperator(
  task_id='get_isrc_meta',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_isrc_meta.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.recommender.model_data_with_meta',
  dag = dag
  )

### Save the model data in GCS
model_data_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
  task_id='model_data_to_gcs',
  source_project_dataset_table='umg-comm-tech-dev.recommender.model_data_with_meta',
  destination_cloud_storage_uris=['gs://us-central1-comm-tech-flow--c62114c2-bucket/data/recommender_data/date_id={{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}/model_data_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}-*.csv'],
  export_format='CSV',
  field_delimiter='\t',
  print_header=False,
  bigquery_conn_id='bigquery_default',
  dag=dag
  )

### Copy over model data to local folder
##download_model_data = BashOperator(
##        task_id='download_model_data',
##        bash_command='gsutil cp -r gs://umg-comm-tech-dev/recommender/model_data/date_id={{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}/model_data_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}-*.csv /home/airflow/gcs/data/recommender_data/',
##        dag=dag
##        )
#gcs_download_operator.GoogleCloudStorageDownloadOperator(
#  task_id='download_model_data',
#  bucket='umg-comm-tech-dev',
#  object= 'recommender/model_data/date_id={{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}/model_data_{{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}-*.csv',
#  filename='/home/airflow/gcs/data/model_data.csv',
#  bigquery_conn_id='google_cloud_storage_default',
#  dag=dag
#  )

### Retrain the model based on the new data
concatenate_model_data = BashOperator(
  task_id ='concatenate_model_data',
  bash_command='python /home/airflow/gcs/dags/app/concatenate_model_data.py /home/airflow/gcs/data/recommender_data/date_id={{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}/ /home/airflow/gcs/data/recommender_data/date_id={{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}/model_data.csv',
  dag=dag
  )

### Update Pandas to the latest version
update_pandas = BashOperator(
        task_id ='update_pandas',
        bash_command='sudo pip3 install --upgrade pandas',
        dag=dag
  )

### Retrain the model based on the new data
retrain_model = BashOperator(
  task_id ='retrain_model',
  bash_command='python /home/airflow/gcs/dags/app/retrain_model.py /home/airflow/gcs/data/recommender_data/date_id={{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}/model_data.csv /home/airflow/gcs/data/model_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl',
  dag=dag
  )

### Transfer the previous model to the dated model file
transfer_old_model = BashOperator(
  task_id='transfer_old_model',
  bash_command="gsutil cp gs://umg-comm-tech-dev/recommender/model/model_latest.pkl gs://umg-comm-tech-dev/recommender/model/model_{{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl",
  dag = dag
  )

### Transfer the model to the recommender bucket as model_latest.pkl
transfer_new_model = BashOperator(
  task_id='transfer_new_model',
  bash_command="gsutil cp /home/airflow/gcs/data/model_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl  gs://umg-comm-tech-dev/recommender/model/model_latest.pkl",
  dag = dag
  )

### Create metadata table

get_epf_meta = BigQueryOperator(
  task_id='get_epf_meta',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/get_epf_meta.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.recommender.track_metadata_augm',
  dag = dag
  )

### Save the meta data in GCS
meta_data_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
  task_id='meta_data_to_gcs',
  source_project_dataset_table='umg-comm-tech-dev.recommender.track_metadata_augm',
  destination_cloud_storage_uris=['gs://umg-comm-tech-dev/recommender/metadata/metadata_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.csv'],
  export_format='CSV',
  field_delimiter='\t',
  print_header=False,
  bigquery_conn_id='bigquery_default',
  dag=dag
  )

### Copy over meta data to local folder
download_meta_data = gcs_download_operator.GoogleCloudStorageDownloadOperator(
  task_id='download_meta_data',
  bucket='umg-comm-tech-dev',
  object= 'recommender/metadata/metadata_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.csv',
  filename='/home/airflow/gcs/data/metadata.csv',
  bigquery_conn_id='google_cloud_storage_default',
  dag=dag
  )

### Save the metadata dictionary pickle
create_new_meta = BashOperator(
  task_id ='create_new_meta',
  bash_command='python /home/airflow/gcs/dags/app/create_new_meta.py /home/airflow/gcs/data/metadata.csv /home/airflow/gcs/data/metadata_dict_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl',
  dag=dag
  )

### Transfer the previous metadata dictionary to the dated file
transfer_old_meta = BashOperator(
  task_id='transfer_old_meta',
  bash_command="gsutil cp gs://umg-comm-tech-dev/recommender/metadata/metadata_dict.pkl gs://umg-comm-tech-dev/recommender/metadata/metadata_dict_{{ macros.ds_format(macros.ds_add( ds, -30), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl",
  dag = dag
  )

### Transfer the new metadata dictionary to the recommender bucket as metadata_dict.pkl
transfer_new_meta = BashOperator(
  task_id='transfer_new_meta',
  bash_command="gsutil cp /home/airflow/gcs/data/metadata_dict_{{ macros.ds_format(macros.ds_add( ds, 0), \'%Y-%m-%d\', \'%Y%m%d\') }}.pkl  gs://umg-comm-tech-dev/recommender/metadata/metadata_dict.pkl",
  dag = dag
  )


### Task dependencies
get_playlists >> get_missing_browse_playlists >> get_isrc_meta >> model_data_to_gcs >> concatenate_model_data >> update_pandas >> retrain_model >> transfer_old_model >> transfer_new_model
get_isrc_meta >> get_epf_meta >> meta_data_to_gcs >> download_meta_data >> create_new_meta >> transfer_old_meta >> transfer_new_meta
