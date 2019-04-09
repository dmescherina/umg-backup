import airflow
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import airflow.macros

import pytz
from datetime import date, timedelta, datetime

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 24),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('fixed_playlists_moments',
  description = 'Creating data for Fixed Playlists moments dashboard',
  schedule_interval='30 08 * * *', # run daily at 8:30am
  max_active_runs=1,
  default_args=args)


### Run the query to get the positive moments from the moments table

get_positive_moments = BigQueryOperator(
  task_id='moments',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/moments.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.moments',
  dag = dag
  )

### Enrich with other metadata

moments_with_meta = BigQueryOperator(
  task_id='moments_with_meta',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/moments_with_meta.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.moments_with_meta',
  dag = dag
  )

### Task dependencies
get_positive_moments >> moments_with_meta

