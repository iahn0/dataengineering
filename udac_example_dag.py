from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

'''
1. Dag configuration 
DAG contains default_args dict, with the following keys:
The DAG does not have dependencies on past runs
On failure, the task are retried 3 times
Retries happen every 5 minutes
Catchup is turned off
Do not email on retry
'''
'''
aws s3 ls s3://udacity-dend/log-data --recursive
start_date is log-data/2018/11/2018-11-01-events.json


aws s3 ls s3://udacity-dend/song-data/A/A/A --recursive
2019-04-04 18:10:03        244 song-data/A/A/A/TRAAAAK128F9318786.json
2019-04-04 18:10:03        303 song-data/A/A/A/TRAAAAV128F421A322.json
2019-04-04 18:10:03        268 song-data/A/A/A/TRAAABD128F429CF47.json

'''

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018,11,1),
    'end_date': datetime(2018,11,3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


'''
Defaults_args are bind to the DAG
The DAG should be scheduled to run once an hour
'''
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1 
        )

'''
start operator
'''
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''
Task to stage JSON data is included in the DAG and uses the StageToRedshift operator
'''


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    provide_context=True,
    execution_date = {{ds}},
    table = "staging_events",
    s3_bucket = "udacity-dend",
    #s3_key ="log-data"
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    provide_context=True,
    execution_date = {{ds}},
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key ="song-data/A/B/C" 
)

'''
Set of tasks using the dimension load operator is in the DAG
Fact table :
songplays

'''

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "songplays",
    sql=SqlQueries.songplay_table_insert
)


'''
Set of tasks using the dimension load operator is in the DAG
Dimension Tables :
user, song, artist, time

The dimension task contains a param to allow switch between append and insert-delete functionality

'''

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "user",
    sql=SqlQueries.user_table_insert
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "song",
    sql=SqlQueries.song_table_insert 
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "artist",
    sql=SqlQueries.artist_table_insert
  
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "time",
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays"
)


'''
end operator
'''

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

'''
dependencies:
Tasks order
'''

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >>  run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table>> run_quality_checks
load_time_dimension_table>> run_quality_checks
run_quality_checks >> end_operator

