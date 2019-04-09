import airflow
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import airflow.macros

import pytz
from datetime import date, timedelta, datetime

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 1),
    'schedule_interval': None ,
    'email': ['daria.meshcherina@umusic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag = DAG('fixed_playlists_vanity_projects',
  description = 'Updating BQ tables to check streams by partners for selected playlists to feed into Data Studio dashboard',
  schedule_interval='30 07 * * *', # run daily at 7:30am
  max_active_runs=1,
  default_args=args)


### Mellow metal stats from Spotify

mellow_metal_spotify = BigQueryOperator(
  task_id='mellow_metal_spotify',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/mellow_metal_spotify.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.melllow_metal_stats',
  dag = dag
  )

### Mellow metal stats from Deezer

mellow_metal_deezer = BigQueryOperator(
  task_id='mellow_metal_deezer',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/mellow_metal_deezer.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.mellow_metal_stats_deezer',
  dag = dag
  )

### Chill, calm, come on down stats from Spotify

come_on_down_spotify = BigQueryOperator(
  task_id='come_on_down_spotify',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/come_on_down_spotify.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.come_on_down_stats',
  dag = dag
  )

### Chill, calm, come on down stats from Spotify

come_on_down_deezer = BigQueryOperator(
  task_id='come_on_down_deezer',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/come_on_down_deezer.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.come_on_down_stats_deezer',
  dag = dag
  )


### 90s Rap stats from Spotify

nineties_rap_spotify = BigQueryOperator(
  task_id='nineties_rap_spotify',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/nineties_rap_spotify.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.nineties_rap_stats',
  dag = dag
  )

### 90s Rap stats from Spotify

nineties_rap_deezer = BigQueryOperator(
  task_id='nineties_rap_deezer',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/nineties_rap_deezer.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.nineties_rap_stats_deezer',
  dag = dag
  )

### Relaxing Dog Music stats from Spotify

consumption_data = BigQueryOperator(
  task_id='consumption_data',
  use_legacy_sql=False,
  allow_large_results=True,
  bql='/sql/consumption_data.sql',
  write_disposition='WRITE_TRUNCATE',
  destination_dataset_table = 'umg-comm-tech-dev.fixed_playlists.stats_swift_consumption_raw',
  dag = dag
  )

