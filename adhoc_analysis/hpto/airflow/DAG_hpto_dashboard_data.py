import airflow
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import airflow.macros

import pytz
from datetime import date, timedelta, datetime

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 14),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('hpto_dashboard_data',
  description = 'Gathering, reformatting and preprocessing data for HPTO dashboard based on Google Analytics',
  schedule_interval='0 0 * * 0', # run on midnight on Mondays
  max_active_runs=1,
  default_args=args)


### Run the query to get the raw data out of GA

hpto_raw_ga = BigQueryOperator(
  task_id='hpto_raw_ga',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/hpto_raw_ga.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.adhoc.htpo_raw_ga',
  dag = dag
  )

### Extract Tier and Territory ids from the raw GA data and re-save

hpto_tier_territory = BigQueryOperator(
  task_id='hpto_tier_territory',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/hpto_tier_territory.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.adhoc.hpto_tier_territory',
  dag = dag
  )

### Aggregate by date, tier and territory

hpto_aggregated = BigQueryOperator(
  task_id='hpto_aggregated',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/hpto_aggregated.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.adhoc.hpto_aggregated',
  dag = dag
  )

### Task dependencies
hpto_raw_ga >> hpto_tier_territory >> hpto_aggregated

