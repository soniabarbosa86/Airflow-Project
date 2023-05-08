from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                              PostgresOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'SoniaBarbosa',
    'start_date': datetime(2019, 1, 12)
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
    )

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    log_json_path = "s3://udacity-dend/log_data"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    song_json_path = "s3://udacity-dend/song_data"
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

Begin_execution >> Stage_events
Begin_execution >> Stage_songs
Stage_events >> Load_songplays_fact_table
Stage_songs >> Load_songplays_fact_table
Load_songplays_fact_table >> Load_song_dim_table
Load_songplays_fact_table >> Load_user_dim_table
Load_songplays_fact_table >> Load_artist_dim_table
Load_songplays_fact_table >> Load_time_dim_table
Load_song_dim_table >> Run_data_quality_checks
Load_user_dim_table >> Run_data_quality_checks
Load_artist_dim_table >> Run_data_quality_checks
Load_time_dim_table >> Run_data_quality_checks
Run_data_quality_checks >> End_execution